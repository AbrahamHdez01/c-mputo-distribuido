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

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
// las réplicas. Las tablas y el usuario demo los crea db/init.sql.
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
	var id int
	err := db.QueryRow(
		"INSERT INTO users (name, email, balance, created_at) VALUES ($1, $2, $3, $4) RETURNING id",
		req.Name, req.Email, balance, now,
	).Scan(&id)
	if err != nil {
		return nil, err
	}

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

// instanceInterceptor manda el hostname de la réplica en un header gRPC,
// para que el gateway pueda mostrar quién atendió la llamada.
func instanceInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (interface{}, error) {
	grpc.SetHeader(ctx, metadata.Pairs("x-instance", hostname))
	return h(ctx, req)
}

func startGRPC() {
	lis, err := net.Listen("tcp", ":9093")
	if err != nil {
		log.Fatalf("gRPC listen error: %v", err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(instanceInterceptor))
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

	err := db.QueryRow("INSERT INTO users (name, email, balance, created_at) VALUES ($1, $2, $3, $4) RETURNING id",
		u.Name, u.Email, u.Balance, u.CreatedAt).Scan(&u.ID)
	if err != nil {
		http.Error(w, "Error guardando usuario (email ya existe?)", http.StatusConflict)
		return
	}

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
		"ID":      "user-service-" + hostname,
		"Name":    "user-service",
		"Address": hostname,
		"Port":    8083,
		"Check": map[string]string{
			"HTTP":     "http://" + hostname + ":8083/health",
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
	req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/deregister/user-service-"+hostname, nil)
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

	http.HandleFunc("/users", withInstance(getUsers))
	http.HandleFunc("/users/create", withInstance(createUser))
	http.HandleFunc("/health", withInstance(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"user-service"}`)
	}))

	log.Println("user-service HTTP corriendo en :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}
