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

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

// hostname identifica a ESTA réplica (en Docker es el id corto del contenedor).
var hostname, _ = os.Hostname()

// withInstance agrega el header X-Instance para ver qué réplica contestó.
func withInstance(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Instance", hostname)
		h(w, r)
	}
}

// initDB se conecta a la base de datos ÚNICA (PostgreSQL) compartida por todas
// las réplicas. Las tablas las crea db/init.sql, no el servicio.
func initDB() {
	var err error
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("No se pudo conectar a PostgreSQL:", err)
	}

	log.Printf("Conectado a PostgreSQL (réplica %s).", hostname)
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
	var id int
	err := db.QueryRow(
		"INSERT INTO orders (symbol, side, price, amount, status, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
		req.Symbol, req.Side, req.Price, req.Amount, "PENDING", now,
	).Scan(&id)
	if err != nil {
		return nil, err
	}

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
//
// Con varias réplicas, el UPDATE ... RETURNING es atómico: si dos réplicas
// intentan completar la misma orden, solo una la recibe de vuelta.
func matchOrders(symbol string, marketPrice float64) {
	rows, err := db.Query(`UPDATE orders SET status = 'FILLED'
		WHERE status = 'PENDING' AND symbol = $1
		  AND ((side = 'BUY' AND price >= $2) OR (side = 'SELL' AND price <= $2))
		RETURNING id, side, price, amount`,
		symbol, marketPrice)
	if err != nil {
		log.Printf("[matching] error completando órdenes: %v", err)
		return
	}

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

// instanceInterceptor manda el hostname de la réplica en un header gRPC,
// para que el gateway pueda mostrar quién atendió la llamada.
func instanceInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (interface{}, error) {
	grpc.SetHeader(ctx, metadata.Pairs("x-instance", hostname))
	return h(ctx, req)
}

func startGRPC() {
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("gRPC listen error: %v", err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(instanceInterceptor))
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

	// Un ID distinto por réplica; si no, la segunda pisaría el registro de la primera.
	payload := map[string]interface{}{
		"ID":      "order-service-" + hostname,
		"Name":    "order-service",
		"Address": hostname,
		"Port":    8081,
		"Check": map[string]string{
			"HTTP":     "http://" + hostname + ":8081/health",
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
		"http://"+consulAddr+"/v1/agent/service/deregister/order-service-"+hostname,
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

	err := db.QueryRow("INSERT INTO orders (symbol, side, price, amount, status, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
		o.Symbol, o.Side, o.Price, o.Amount, o.Status, o.CreatedAt).Scan(&o.ID)
	if err != nil {
		http.Error(w, "Error guardando orden", http.StatusInternalServerError)
		return
	}

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

	http.HandleFunc("/orders", withInstance(getOrders))
	http.HandleFunc("/orders/create", withInstance(createOrder))
	http.HandleFunc("/health", withInstance(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"order-service"}`)
	}))

	log.Println("order-service HTTP corriendo en :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
