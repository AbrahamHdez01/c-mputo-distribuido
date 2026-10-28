package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	pb "gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

// grpcTargets: nombre DNS de cada servicio. En Docker, el nombre resuelve a las
// IPs de TODAS las réplicas, y gRPC con round_robin reparte las llamadas entre
// ellas (una conexión persistente por servicio, no una por petición).
var grpcTargets = map[string]string{
	"order-service": "order-service:9091",
	"price-service": "price-service:9092",
	"user-service":  "user-service:9093",
}

var grpcConns = map[string]*grpc.ClientConn{}

const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

func initGRPC() {
	for name, target := range grpcTargets {
		conn, err := grpc.NewClient("dns:///"+target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(roundRobin))
		if err != nil {
			log.Fatalf("[gateway] no se pudo crear el cliente gRPC de %s: %v", name, err)
		}
		grpcConns[name] = conn
	}
}

// callGRPC ejecuta una llamada gRPC y responde en JSON. X-Served-By lleva el
// hostname de la réplica que atendió (lo manda el servidor en el header gRPC
// "x-instance"), así se ve el reparto entre réplicas.
func callGRPC(w http.ResponseWriter, service string, okStatus int,
	call func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error)) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var hdr metadata.MD
	out, err := call(ctx, grpcConns[service], grpc.Header(&hdr))
	if err != nil {
		http.Error(w, "gRPC error: "+err.Error(), http.StatusBadGateway)
		return
	}

	instance := "desconocida"
	if v := hdr.Get("x-instance"); len(v) > 0 {
		instance = v[0]
	}
	log.Printf("[gateway] gRPC %s → atendió %s", service, instance)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Transport", "grpc")
	w.Header().Set("X-Served-By", instance)
	w.WriteHeader(okStatus)
	json.NewEncoder(w).Encode(out)
}

func grpcOrders(w http.ResponseWriter, r *http.Request) {
	callGRPC(w, "order-service", http.StatusOK, func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error) {
		resp, err := pb.NewOrderServiceClient(conn).ListOrders(ctx, &pb.ListOrdersRequest{}, opts...)
		if err != nil {
			return nil, err
		}
		return resp.Orders, nil
	})
}

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
	callGRPC(w, "order-service", http.StatusCreated, func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error) {
		return pb.NewOrderServiceClient(conn).CreateOrder(ctx, &req, opts...)
	})
}

func grpcPrices(w http.ResponseWriter, r *http.Request) {
	callGRPC(w, "price-service", http.StatusOK, func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error) {
		resp, err := pb.NewPriceServiceClient(conn).ListPrices(ctx, &pb.ListPricesRequest{}, opts...)
		if err != nil {
			return nil, err
		}
		return resp.Prices, nil
	})
}

func grpcUsers(w http.ResponseWriter, r *http.Request) {
	callGRPC(w, "user-service", http.StatusOK, func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error) {
		resp, err := pb.NewUserServiceClient(conn).ListUsers(ctx, &pb.ListUsersRequest{}, opts...)
		if err != nil {
			return nil, err
		}
		return resp.Users, nil
	})
}

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
	callGRPC(w, "user-service", http.StatusCreated, func(ctx context.Context, conn *grpc.ClientConn, opts ...grpc.CallOption) (interface{}, error) {
		return pb.NewUserServiceClient(conn).CreateUser(ctx, &req, opts...)
	})
}

// grpcStatus — resuelve el DNS de cada servicio y muestra las IPs de sus réplicas
func grpcStatus(w http.ResponseWriter, r *http.Request) {
	type GRPCInfo struct {
		Service  string   `json:"service"`
		Target   string   `json:"target"`
		Replicas []string `json:"replicas"`
	}
	result := []GRPCInfo{}
	for name, target := range grpcTargets {
		host, _, _ := net.SplitHostPort(target)
		ips, _ := net.LookupHost(host)
		result = append(result, GRPCInfo{Service: name, Target: target, Replicas: ips})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	initGRPC()

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
