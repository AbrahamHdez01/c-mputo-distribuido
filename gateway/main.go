package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

// ServicePool maneja las instancias de un servicio con round robin
type ServicePool struct {
	instances []string
	counter   uint64
}

// Next devuelve la siguiente instancia disponible
func (sp *ServicePool) Next() string {
	idx := atomic.AddUint64(&sp.counter, 1) % uint64(len(sp.instances))
	return sp.instances[idx]
}

// registry mapea cada servicio a sus instancias (service discovery)
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

// proxyTo redirige la petición al servicio indicado
func proxyTo(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _ := url.Parse(target)
		proxy := httputil.NewSingleHostReverseProxy(u)
		r.Header.Set("X-Forwarded-By", "gateway")
		w.Header().Set("X-Served-By", target)
		proxy.ServeHTTP(w, r)
	}
}

// route enruta la petición al servicio correcto según el path
func route(w http.ResponseWriter, r *http.Request) {
	// headers CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	log.Printf("[gateway] %s %s", r.Method, r.URL.Path)

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

// healthCheck consulta el estado de cada servicio
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

// services devuelve el registro de servicios
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

func main() {
	http.HandleFunc("/orders", route)
	http.HandleFunc("/orders/", route)
	http.HandleFunc("/orders/create", route)
	http.HandleFunc("/prices", route)
	http.HandleFunc("/prices/", route)
	http.HandleFunc("/users", route)
	http.HandleFunc("/users/", route)
	http.HandleFunc("/users/create", route)
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/services", services)

	fmt.Println("[gateway] Corriendo en :8080")
	fmt.Println("[gateway] Service Discovery en /services")
	fmt.Println("[gateway] Health Check en /health")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
