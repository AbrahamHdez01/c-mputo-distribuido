package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "order-service/proto"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Order es la estructura de una orden de trading
type Order struct {
	ID        int       `json:"id"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", "/data/orders.db")
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS orders (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		symbol     TEXT    NOT NULL,
		side       TEXT    NOT NULL,
		price      REAL    NOT NULL,
		amount     REAL    NOT NULL,
		status     TEXT    NOT NULL DEFAULT 'PENDING',
		created_at DATETIME NOT NULL
	)`)
	if err != nil {
		log.Fatal("Error creando tabla orders:", err)
	}

	log.Println("Base de datos de órdenes lista.")
}

// ─── gRPC server ──────────────────────────────────────────────────────────────

type orderGRPCServer struct {
	pb.UnimplementedOrderServiceServer
}

func (s *orderGRPCServer) ListOrders(_ context.Context, _ *pb.ListOrdersRequest) (*pb.ListOrdersResponse, error) {
	rows, err := db.Query("SELECT id, symbol, side, price, amount, status, created_at FROM orders")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []*pb.OrderResponse
	for rows.Next() {
		var (
			id        int
			symbol    string
			side      string
			price     float64
			amount    float64
			status    string
			createdAt time.Time
		)
		rows.Scan(&id, &symbol, &side, &price, &amount, &status, &createdAt)
		orders = append(orders, &pb.OrderResponse{
			Id:        int32(id),
			Symbol:    symbol,
			Side:      side,
			Price:     price,
			Amount:    amount,
			Status:    status,
			CreatedAt: createdAt.Format(time.RFC3339),
		})
	}
	return &pb.ListOrdersResponse{Orders: orders}, nil
}

func (s *orderGRPCServer) CreateOrder(_ context.Context, req *pb.CreateOrderRequest) (*pb.OrderResponse, error) {
	now := time.Now()
	res, err := db.Exec(
		"INSERT INTO orders (symbol, side, price, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		req.Symbol, req.Side, req.Price, req.Amount, "PENDING", now,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()

	order := Order{
		ID:        int(id),
		Symbol:    req.Symbol,
		Side:      req.Side,
		Price:     req.Price,
		Amount:    req.Amount,
		Status:    "PENDING",
		CreatedAt: now,
	}

	// Kafka: avisamos al resto del sistema que entró una orden nueva.
	publishEvent(TopicOrdersCreated, order.Symbol, order)

	return &pb.OrderResponse{
		Id:        int32(id),
		Symbol:    req.Symbol,
		Side:      req.Side,
		Price:     req.Price,
		Amount:    req.Amount,
		Status:    "PENDING",
		CreatedAt: now.Format(time.RFC3339),
	}, nil
}

// ─── Matching asíncrono (consumidor de prices.updated) ────────────────────────
//
// Nadie llama esta función por HTTP ni por gRPC: se dispara sola cada vez que
// el price-service publica un precio nuevo en Kafka. Eso es el desacoplamiento:
// el price-service no sabe que el order-service existe.

// onPriceUpdated se ejecuta con cada mensaje de la cola 'order-matcher'.
func onPriceUpdated(ev Event) {
	var p struct {
		Symbol string  `json:"symbol"`
		Price  float64 `json:"price"`
	}
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		log.Printf("[matching] payload inválido: %v", err)
		return
	}
	matchOrders(p.Symbol, p.Price)
}

// matchOrders completa las órdenes PENDING que el precio de mercado ya alcanzó:
//   • una orden BUY  se completa si el mercado bajó hasta su precio o menos
//   • una orden SELL se completa si el mercado subió hasta su precio o más
func matchOrders(symbol string, marketPrice float64) {
	rows, err := db.Query(`SELECT id, side, price, amount FROM orders
		WHERE status = 'PENDING' AND symbol = ?
		  AND ((side = 'BUY' AND price >= ?) OR (side = 'SELL' AND price <= ?))`,
		symbol, marketPrice, marketPrice)
	if err != nil {
		log.Printf("[matching] error buscando órdenes: %v", err)
		return
	}

	// Juntamos primero y actualizamos después: SQLite no deja escribir
	// mientras un SELECT sigue abierto.
	type pending struct {
		id     int
		side   string
		price  float64
		amount float64
	}
	var matched []pending
	for rows.Next() {
		var o pending
		rows.Scan(&o.id, &o.side, &o.price, &o.amount)
		matched = append(matched, o)
	}
	rows.Close()

	for _, o := range matched {
		if _, err := db.Exec("UPDATE orders SET status = 'FILLED' WHERE id = ?", o.id); err != nil {
			log.Printf("[matching] error actualizando orden #%d: %v", o.id, err)
			continue
		}

		log.Printf("[matching] orden #%d %s %s @ %.2f completada (mercado %.2f)",
			o.id, o.side, symbol, o.price, marketPrice)

		// El resultado del matching también es un evento: lo publicamos.
		publishEvent(TopicOrdersFilled, symbol, map[string]interface{}{
			"id":           o.id,
			"symbol":       symbol,
			"side":         o.side,
			"order_price":  o.price,
			"market_price": marketPrice,
			"amount":       o.amount,
		})
	}
}

func startGRPC() {
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("gRPC listen error: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterOrderServiceServer(srv, &orderGRPCServer{})
	reflection.Register(srv) // habilita grpcurl sin -proto flag
	log.Println("order-service gRPC corriendo en :9091")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC serve error: %v", err)
	}
}

// ─── HTTP handlers (sin cambios) ──────────────────────────────────────────────

func registerConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}

	payload := map[string]interface{}{
		"ID":      "order-service",
		"Name":    "order-service",
		"Address": "order-service",
		"Port":    8081,
		"Check": map[string]string{
			"HTTP":     "http://order-service:8081/health",
			"Interval": "10s",
			"Timeout":  "3s",
		},
	}

	body, _ := json.Marshal(payload)

	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/register", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			log.Println("Registrado en Consul correctamente")
			return
		}
		log.Printf("Esperando Consul... intento %d/10", i+1)
		time.Sleep(3 * time.Second)
	}
	log.Println("No se pudo registrar en Consul (el servicio seguirá funcionando)")
}

func deregisterConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}

	req, _ := http.NewRequest(
		http.MethodPut,
		"http://"+consulAddr+"/v1/agent/service/deregister/order-service",
		nil,
	)
	client := &http.Client{Timeout: 3 * time.Second}
	client.Do(req)
	log.Println("Desregistrado de Consul")
}

func getOrders(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, symbol, side, price, amount, status, created_at FROM orders")
	if err != nil {
		http.Error(w, "Error leyendo órdenes", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var o Order
		rows.Scan(&o.ID, &o.Symbol, &o.Side, &o.Price, &o.Amount, &o.Status, &o.CreatedAt)
		orders = append(orders, o)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var o Order
	if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}
	o.Status = "PENDING"
	o.CreatedAt = time.Now()

	res, err := db.Exec("INSERT INTO orders (symbol, side, price, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		o.Symbol, o.Side, o.Price, o.Amount, o.Status, o.CreatedAt)
	if err != nil {
		http.Error(w, "Error guardando orden", http.StatusInternalServerError)
		return
	}

	id, _ := res.LastInsertId()
	o.ID = int(id)

	// Kafka: mismo evento que en el camino gRPC.
	publishEvent(TopicOrdersCreated, o.Symbol, o)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(o)
}

func main() {
	initDB()

	// Kafka: productor + la cola que escucha los precios para hacer matching.
	initKafka("order-service")
	startConsumer(GroupOrderMatcher, TopicPricesUpdated, onPriceUpdated)

	go registerConsul()
	go startGRPC()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		deregisterConsul()
		os.Exit(0)
	}()

	http.HandleFunc("/orders", getOrders)
	http.HandleFunc("/orders/create", createOrder)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"order-service"}`)
	})

	log.Println("order-service HTTP corriendo en :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
