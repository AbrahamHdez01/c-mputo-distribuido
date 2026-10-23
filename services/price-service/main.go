package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "price-service/proto"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Price guarda el precio actual de un par de monedas
type Price struct {
	ID        int       `json:"id"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	UpdatedAt time.Time `json:"updated_at"`
}

var db *sql.DB

var seedPrices = map[string]float64{
	"BTC/USD": 65000.0,
	"ETH/USD": 3500.0,
	"SOL/USD": 180.0,
}

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", "/data/prices.db")
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS prices (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		symbol     TEXT    NOT NULL UNIQUE,
		price      REAL    NOT NULL,
		updated_at DATETIME NOT NULL
	)`)
	if err != nil {
		log.Fatal("Error creando tabla prices:", err)
	}

	for symbol, price := range seedPrices {
		db.Exec(`INSERT OR IGNORE INTO prices (symbol, price, updated_at) VALUES (?, ?, ?)`,
			symbol, price, time.Now())
	}

	log.Println("Base de datos de precios lista.")
}

func simulateMarket() {
	for {
		time.Sleep(5 * time.Second)
		rows, _ := db.Query("SELECT id, symbol, price FROM prices")
		defer rows.Close()

		type row struct {
			id     int
			symbol string
			price  float64
		}
		var toUpdate []row
		for rows.Next() {
			var r row
			rows.Scan(&r.id, &r.symbol, &r.price)
			toUpdate = append(toUpdate, r)
		}
		rows.Close()

		for _, r := range toUpdate {
			change := (rand.Float64() - 0.5) * 0.01
			newPrice := r.price * (1 + change)
			now := time.Now()
			db.Exec("UPDATE prices SET price = ?, updated_at = ? WHERE id = ?",
				newPrice, now, r.id)

			// Kafka: cada precio nuevo es un evento. Un mensaje por símbolo,
			// con el símbolo como key para que no se desordenen entre sí.
			publishEvent(TopicPricesUpdated, r.symbol, map[string]interface{}{
				"symbol":     r.symbol,
				"price":      newPrice,
				"updated_at": now,
			})
		}
	}
}

// ─── gRPC server ──────────────────────────────────────────────────────────────

type priceGRPCServer struct {
	pb.UnimplementedPriceServiceServer
}

func (s *priceGRPCServer) ListPrices(_ context.Context, _ *pb.ListPricesRequest) (*pb.ListPricesResponse, error) {
	rows, err := db.Query("SELECT id, symbol, price, updated_at FROM prices")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var prices []*pb.PriceResponse
	for rows.Next() {
		var (
			id        int
			symbol    string
			price     float64
			updatedAt time.Time
		)
		rows.Scan(&id, &symbol, &price, &updatedAt)
		prices = append(prices, &pb.PriceResponse{
			Id:        int32(id),
			Symbol:    symbol,
			Price:     price,
			UpdatedAt: updatedAt.Format(time.RFC3339),
		})
	}
	return &pb.ListPricesResponse{Prices: prices}, nil
}

func startGRPC() {
	lis, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatalf("gRPC listen error: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterPriceServiceServer(srv, &priceGRPCServer{})
	reflection.Register(srv)
	log.Println("price-service gRPC corriendo en :9092")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC serve error: %v", err)
	}
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

func getPrices(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, symbol, price, updated_at FROM prices")
	if err != nil {
		http.Error(w, "Error leyendo precios", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	prices := []Price{}
	for rows.Next() {
		var p Price
		rows.Scan(&p.ID, &p.Symbol, &p.Price, &p.UpdatedAt)
		prices = append(prices, p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(prices)
}

func registerConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	payload := map[string]interface{}{
		"ID":      "price-service",
		"Name":    "price-service",
		"Address": "price-service",
		"Port":    8082,
		"Check": map[string]string{
			"HTTP":     "http://price-service:8082/health",
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
	req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/deregister/price-service", nil)
	client := &http.Client{Timeout: 3 * time.Second}
	client.Do(req)
	log.Println("Desregistrado de Consul")
}

func main() {
	initDB()

	// Kafka: este servicio solo produce (publica en prices.updated).
	initKafka("price-service")

	go simulateMarket()
	go registerConsul()
	go startGRPC()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		deregisterConsul()
		os.Exit(0)
	}()

	http.HandleFunc("/prices", getPrices)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"price-service"}`)
	})

	log.Println("price-service HTTP corriendo en :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
