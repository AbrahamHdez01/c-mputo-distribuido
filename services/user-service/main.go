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

	pb "user-service/proto"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

// ─── gRPC server ──────────────────────────────────────────────────────────────

type userGRPCServer struct {
	pb.UnimplementedUserServiceServer
}

func (s *userGRPCServer) ListUsers(_ context.Context, _ *pb.ListUsersRequest) (*pb.ListUsersResponse, error) {
	rows, err := db.Query("SELECT id, name, email, balance, created_at FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*pb.UserResponse
	for rows.Next() {
		var (
			id        int
			name      string
			email     string
			balance   float64
			createdAt time.Time
		)
		rows.Scan(&id, &name, &email, &balance, &createdAt)
		users = append(users, &pb.UserResponse{
			Id:        int32(id),
			Name:      name,
			Email:     email,
			Balance:   balance,
			CreatedAt: createdAt.Format(time.RFC3339),
		})
	}
	return &pb.ListUsersResponse{Users: users}, nil
}

func (s *userGRPCServer) CreateUser(_ context.Context, req *pb.CreateUserRequest) (*pb.UserResponse, error) {
	balance := req.Balance
	if balance == 0 {
		balance = 10000.0
	}
	now := time.Now()
	res, err := db.Exec(
		"INSERT INTO users (name, email, balance, created_at) VALUES (?, ?, ?, ?)",
		req.Name, req.Email, balance, now,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()

	// Kafka: avisamos que se registró un trader nuevo.
	publishEvent(TopicUsersRegistered, req.Email, User{
		ID:        int(id),
		Name:      req.Name,
		Email:     req.Email,
		Balance:   balance,
		CreatedAt: now,
	})

	return &pb.UserResponse{
		Id:        int32(id),
		Name:      req.Name,
		Email:     req.Email,
		Balance:   balance,
		CreatedAt: now.Format(time.RFC3339),
	}, nil
}

func startGRPC() {
	lis, err := net.Listen("tcp", ":9093")
	if err != nil {
		log.Fatalf("gRPC listen error: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterUserServiceServer(srv, &userGRPCServer{})
	reflection.Register(srv)
	log.Println("user-service gRPC corriendo en :9093")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC serve error: %v", err)
	}
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

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

	// Kafka: mismo evento que en el camino gRPC.
	publishEvent(TopicUsersRegistered, u.Email, u)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(u)
}

func registerConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	payload := map[string]interface{}{
		"ID":      "user-service",
		"Name":    "user-service",
		"Address": "user-service",
		"Port":    8083,
		"Check": map[string]string{
			"HTTP":     "http://user-service:8083/health",
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
	req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/deregister/user-service", nil)
	client := &http.Client{Timeout: 3 * time.Second}
	client.Do(req)
	log.Println("Desregistrado de Consul")
}

func main() {
	initDB()

	// Kafka: este servicio solo produce (publica en users.registered).
	initKafka("user-service")

	go registerConsul()
	go startGRPC()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		deregisterConsul()
		os.Exit(0)
	}()

	http.HandleFunc("/users", getUsers)
	http.HandleFunc("/users/create", createUser)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"user-service"}`)
	})

	log.Println("user-service HTTP corriendo en :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}
