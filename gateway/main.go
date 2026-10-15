package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	pb "gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ─── HTTP Load Balancer (Round Robin) ─────────────────────────────────────────

// ServicePool maneja las instancias HTTP de un servicio con round robin
type ServicePool struct {
	instances []string
	counter   uint64
}

func (sp *ServicePool) Next() string {
	idx := atomic.AddUint64(&sp.counter, 1) % uint64(len(sp.instances))
	return sp.instances[idx]
}

var registry = map[string]*ServicePool{
	"order-service": {instances: []string{
		"http://order-service:8081",
	}},
	"price-service": {instances: []string{
		"http://price-service:8082",
	}},
	"user-service": {instances: []string{
		"http://user-service:8083",
	}},
}

func proxyTo(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _ := url.Parse(target)
		proxy := httputil.NewSingleHostReverseProxy(u)
		r.Header.Set("X-Forwarded-By", "gateway")
		w.Header().Set("X-Served-By", target)
		proxy.ServeHTTP(w, r)
	}
}

func route(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	log.Printf("[gateway] HTTP %s %s", r.Method, r.URL.Path)

	switch {
	case len(r.URL.Path) >= 7 && r.URL.Path[:7] == "/orders":
		pool := registry["order-service"]
		proxyTo(pool.Next())(w, r)

	case len(r.URL.Path) >= 7 && r.URL.Path[:7] == "/prices":
		pool := registry["price-service"]
		proxyTo(pool.Next())(w, r)

	case len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/users":
		pool := registry["user-service"]
		proxyTo(pool.Next())(w, r)

	default:
		http.NotFound(w, r)
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	type ServiceStatus struct {
		Service string `json:"service"`
		URL     string `json:"url"`
		Status  string `json:"status"`
	}

	results := []ServiceStatus{}
	client := &http.Client{Timeout: 2 * time.Second}

	for name, pool := range registry {
		for _, instance := range pool.instances {
			status := "UP"
			resp, err := client.Get(instance + "/health")
			if err != nil || resp.StatusCode != 200 {
				status = "DOWN"
			}
			if resp != nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			results = append(results, ServiceStatus{
				Service: name,
				URL:     instance,
				Status:  status,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func services(w http.ResponseWriter, r *http.Request) {
	type ServiceInfo struct {
		Service   string   `json:"service"`
		Instances []string `json:"instances"`
	}

	result := []ServiceInfo{}
	for name, pool := range registry {
		result = append(result, ServiceInfo{
			Service:   name,
			Instances: pool.instances,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ─── gRPC Load Balancer (Round Robin) ─────────────────────────────────────────

// grpcPool maneja los endpoints gRPC de un servicio con round robin
type grpcPool struct {
	addrs   []string
	counter uint64
}

func (p *grpcPool) next() string {
	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.addrs))
	return p.addrs[idx]
}

var grpcRegistry = map[string]*grpcPool{
	"order-service": {addrs: []string{"order-service:9091"}},
	"price-service": {addrs: []string{"price-service:9092"}},
	"user-service":  {addrs: []string{"user-service:9093"}},
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// grpcOrders — lista órdenes vía gRPC
func grpcOrders(w http.ResponseWriter, r *http.Request) {
	addr := grpcRegistry["order-service"].next()
	log.Printf("[gateway] gRPC ListOrders → %s", addr)

	conn, err := dialGRPC(addr)
	if err != nil {
		http.Error(w, "gRPC connection error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pb.NewOrderServiceClient(conn).ListOrders(ctx, &pb.ListOrdersRequest{})
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", addr)
	json.NewEncoder(w).Encode(resp.Orders)
}

// grpcCreateOrder — crea orden vía gRPC
func grpcCreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var req pb.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	addr := grpcRegistry["order-service"].next()
	log.Printf("[gateway] gRPC CreateOrder → %s", addr)

	conn, err := dialGRPC(addr)
	if err != nil {
		http.Error(w, "gRPC connection error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pb.NewOrderServiceClient(conn).CreateOrder(ctx, &req)
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", addr)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// grpcPrices — lista precios vía gRPC
func grpcPrices(w http.ResponseWriter, r *http.Request) {
	addr := grpcRegistry["price-service"].next()
	log.Printf("[gateway] gRPC ListPrices → %s", addr)

	conn, err := dialGRPC(addr)
	if err != nil {
		http.Error(w, "gRPC connection error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pb.NewPriceServiceClient(conn).ListPrices(ctx, &pb.ListPricesRequest{})
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", addr)
	json.NewEncoder(w).Encode(resp.Prices)
}

// grpcUsers — lista usuarios vía gRPC
func grpcUsers(w http.ResponseWriter, r *http.Request) {
	addr := grpcRegistry["user-service"].next()
	log.Printf("[gateway] gRPC ListUsers → %s", addr)

	conn, err := dialGRPC(addr)
	if err != nil {
		http.Error(w, "gRPC connection error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pb.NewUserServiceClient(conn).ListUsers(ctx, &pb.ListUsersRequest{})
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", addr)
	json.NewEncoder(w).Encode(resp.Users)
}

// grpcCreateUser — crea usuario vía gRPC
func grpcCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var req pb.CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	addr := grpcRegistry["user-service"].next()
	log.Printf("[gateway] gRPC CreateUser → %s", addr)

	conn, err := dialGRPC(addr)
	if err != nil {
		http.Error(w, "gRPC connection error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pb.NewUserServiceClient(conn).CreateUser(ctx, &req)
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", addr)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// grpcStatus — muestra el registro gRPC con sus endpoints
func grpcStatus(w http.ResponseWriter, r *http.Request) {
	type GRPCInfo struct {
		Service string   `json:"service"`
		Addrs   []string `json:"addrs"`
	}
	result := []GRPCInfo{}
	for name, pool := range grpcRegistry {
		result = append(result, GRPCInfo{Service: name, Addrs: pool.addrs})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	// ── rutas HTTP proxy (transporte original) ──
	http.HandleFunc("/orders", route)
	http.HandleFunc("/orders/", route)
	http.HandleFunc("/orders/create", route)
	http.HandleFunc("/prices", route)
	http.HandleFunc("/prices/", route)
	http.HandleFunc("/users", route)
	http.HandleFunc("/users/", route)
	http.HandleFunc("/users/create", route)

	// ── rutas gRPC (nuevo transporte) ──
	http.HandleFunc("/grpc/orders", grpcOrders)
	http.HandleFunc("/grpc/orders/create", grpcCreateOrder)
	http.HandleFunc("/grpc/prices", grpcPrices)
	http.HandleFunc("/grpc/users", grpcUsers)
	http.HandleFunc("/grpc/users/create", grpcCreateUser)

	// ── utilitarios ──
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/services", services)
	http.HandleFunc("/grpc/status", grpcStatus)

	fmt.Println("[gateway] Corriendo en :8080")
	fmt.Println("[gateway] HTTP  proxy → /orders  /prices  /users")
	fmt.Println("[gateway] gRPC  proxy → /grpc/orders  /grpc/prices  /grpc/users")
	fmt.Println("[gateway] Util        → /health  /services  /grpc/status")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
