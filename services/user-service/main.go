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

// User es la estructura de un trader registrado
type User struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Balance   float64   `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", "/data/users.db")
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		name       TEXT    NOT NULL,
		email      TEXT    NOT NULL UNIQUE,
		balance    REAL    NOT NULL DEFAULT 10000.0,
		created_at DATETIME NOT NULL
	)`)
	if err != nil {
		log.Fatal("Error creando tabla users:", err)
	}

	db.Exec(`INSERT OR IGNORE INTO users (name, email, balance, created_at) VALUES (?, ?, ?, ?)`,
		"Trader Demo", "demo@mercado.com", 10000.0, time.Now())

	log.Println("Base de datos de usuarios lista.")
}

// getUsers devuelve todos los usuarios
func getUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, name, email, balance, created_at FROM users")
	if err != nil {
		http.Error(w, "Error leyendo usuarios", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	users := []User{}
	for rows.Next() {
		var u User
		rows.Scan(&u.ID, &u.Name, &u.Email, &u.Balance, &u.CreatedAt)
		users = append(users, u)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

// createUser registra un nuevo trader
func createUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var u User
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}
	if u.Balance == 0 {
		u.Balance = 10000.0
	}
	u.CreatedAt = time.Now()

	res, err := db.Exec("INSERT INTO users (name, email, balance, created_at) VALUES (?, ?, ?, ?)",
		u.Name, u.Email, u.Balance, u.CreatedAt)
	if err != nil {
		http.Error(w, "Error guardando usuario (email ya existe?)", http.StatusConflict)
		return
	}

	id, _ := res.LastInsertId()
	u.ID = int(id)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(u)
}

func main() {
	initDB()

	http.HandleFunc("/users", getUsers)
	http.HandleFunc("/users/create", createUser)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"user-service"}`)
	})

	log.Println("user-service corriendo en :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}
