package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Price guarda el precio actual de un par de monedas
type Price struct {
	ID        int       `json:"id"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	UpdatedAt time.Time `json:"updated_at"`
}

var db *sql.DB

// precios con los que arranca el sistema
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

// simulateMarket aplica una variación aleatoria a los precios cada 5 segundos
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
			change := (rand.Float64() - 0.5) * 0.01 // ±0.5%
			newPrice := r.price * (1 + change)
			db.Exec("UPDATE prices SET price = ?, updated_at = ? WHERE id = ?",
				newPrice, time.Now(), r.id)
		}
	}
}

// getPrices devuelve los precios actuales
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

func main() {
	initDB()

	go simulateMarket()

	http.HandleFunc("/prices", getPrices)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"price-service"}`)
	})

	log.Println("price-service corriendo en :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
