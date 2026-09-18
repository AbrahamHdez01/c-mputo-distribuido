package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Order representa una orden de compra/venta en el mercado.
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

	// Crear la tabla si no existe
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

// GET /orders — devuelve todas las órdenes en JSON
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

// POST /orders — crea una nueva orden
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(o)
}

func main() {
	initDB()

	http.HandleFunc("/orders", getOrders)
	http.HandleFunc("/orders/create", createOrder)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"order-service"}`)
	})

	log.Println("order-service corriendo en :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
