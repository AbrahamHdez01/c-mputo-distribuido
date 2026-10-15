# Simulador de Mercado — Microservicios

Proyecto de **Cómputo Distribuido**.
Sistema distribuido que simula un mercado financiero usando microservicios con Docker.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────┐
│  USUARIO (navegador)                                          │
└─────────────────────┬────────────────────────────────────────┘
                      │ http://localhost:3000
                      ▼
┌──────────────────────────────────────────────────────────────┐
│  FRONTEND  (Nginx — puerto 3000)                             │
│  index.html — interfaz web con JS puro                       │
└─────────────────────┬────────────────────────────────────────┘
                      │ http://localhost:8080
                      ▼
┌──────────────────────────────────────────────────────────────┐
│  NGINX GATEWAY  (Nginx — puerto 8080)                        │
│  • Reverse Proxy  • Load Balancer HTTP Round Robin           │
└──────┬──────────────┬──────────────────┬─────────────────────┘
       │ HTTP         │ HTTP             │ HTTP
       ▼              ▼                  ▼
  ┌──────────┐  ┌───────────┐  ┌─────────────┐
  │  order   │  │   price   │  │    user     │
  │ service  │  │  service  │  │   service   │
  │ :8081    │  │   :8082   │  │    :8083    │
  │ gRPC9091 │  │  gRPC9092 │  │  gRPC9093  │
  │  SQLite  │  │   SQLite  │  │   SQLite    │
  └──────────┘  └───────────┘  └─────────────┘
       ▲              ▲                  ▲
       │ gRPC         │ gRPC             │ gRPC
       └──────────────┴──────────────────┘
                      │
┌──────────────────────────────────────────────────────────────┐
│  GO GATEWAY  (puerto 8090)                                   │
│  • gRPC Client Round Robin Load Balancer                     │
│  • HTTP→gRPC proxy (/grpc/*)                                 │
└──────────────────────────────────────────────────────────────┘
```

---

## Estructura del proyecto

```
simulador-mercado/
├── docker-compose.yml
├── setup.sh
│
├── proto/                      ← Definición Protobuf compartida
│   ├── market.proto            ← Contrato gRPC de los 3 servicios
│   ├── market.pb.go            ← Generado por protoc
│   └── market_grpc.pb.go       ← Generado por protoc
│
├── frontend/                   ← Contenedor 1: UI (Nginx)
│   ├── Dockerfile
│   └── index.html
│
├── nginx-gateway/              ← Contenedor 2: Reverse Proxy + LB HTTP (Nginx)
│   ├── Dockerfile
│   └── nginx.conf
│
├── gateway/                    ← Contenedor 3: Go Gateway gRPC LB
│   ├── Dockerfile
│   ├── go.mod / go.sum
│   ├── main.go                 ← HTTP→gRPC proxy con round robin
│   └── proto/                  ← Copia del código generado
│
└── services/
    ├── order-service/          ← Contenedor 4: Órdenes (HTTP :8081 + gRPC :9091)
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   ├── main.go
    │   └── proto/
    ├── price-service/          ← Contenedor 5: Precios  (HTTP :8082 + gRPC :9092)
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   ├── main.go
    │   └── proto/
    └── user-service/           ← Contenedor 6: Usuarios (HTTP :8083 + gRPC :9093)
        ├── Dockerfile
        ├── go.mod / go.sum
        ├── main.go
        └── proto/
```

---

## Cómo correr el proyecto

### Requisito: Docker Desktop

Descárgalo en: https://www.docker.com/products/docker-desktop

### Opción A — Script automático

```bash
chmod +x setup.sh
./setup.sh
```

### Opción B — Manual

```bash
docker compose up --build -d
docker compose logs -f
docker compose down
```

---

## URLs

| Componente | URL |
|---|---|
| **Frontend** | http://localhost:3000 |
| **Nginx Gateway (HTTP)** | http://localhost:8080 |
| **Go Gateway (gRPC-proxy)** | http://localhost:8090 |
| **Health Check** | http://localhost:8080/health |
| **Service Discovery** | http://localhost:8080/services |
| **gRPC Status** | http://localhost:8090/grpc/status |
| **Consul UI** | http://localhost:8500 |

Los microservicios solo son accesibles desde dentro de la red Docker (por alguno de los gateways).

---

## API — Endpoints HTTP (todos via nginx-gateway :8080)

| Método | Ruta | Descripción |
|---|---|---|
| GET | /orders | Lista todas las órdenes |
| POST | /orders/create | Crea una orden |
| GET | /prices | Precios actuales |
| GET | /users | Lista traders |
| POST | /users/create | Registra un trader |
| GET | /health | Estado del gateway |
| GET | /services | Servicios registrados |

## API — Endpoints gRPC-proxy (via go-gateway :8090)

| Método | Ruta | Descripción | Transporte interno |
|---|---|---|---|
| GET | /grpc/orders | Lista órdenes | HTTP → gRPC |
| POST | /grpc/orders/create | Crea una orden | HTTP → gRPC |
| GET | /grpc/prices | Precios actuales | HTTP → gRPC |
| GET | /grpc/users | Lista usuarios | HTTP → gRPC |
| POST | /grpc/users/create | Registra trader | HTTP → gRPC |
| GET | /grpc/status | Registro gRPC interno | - |

Todas las respuestas llevan el header `X-Transport: grpc` para confirmar el transporte.

---

## gRPC — Implementación

### Contrato Protobuf (`proto/market.proto`)

```protobuf
syntax = "proto3";
package market;

service OrderService {
  rpc ListOrders   (ListOrdersRequest)  returns (ListOrdersResponse);
  rpc CreateOrder  (CreateOrderRequest) returns (OrderResponse);
}

service PriceService {
  rpc ListPrices (ListPricesRequest) returns (ListPricesResponse);
}

service UserService {
  rpc ListUsers  (ListUsersRequest)  returns (ListUsersResponse);
  rpc CreateUser (CreateUserRequest) returns (UserResponse);
}
```

### Puertos gRPC

| Servicio | Puerto HTTP | Puerto gRPC |
|---|---|---|
| order-service | 8081 | **9091** |
| price-service | 8082 | **9092** |
| user-service | 8083 | **9093** |

### Server Reflection

Todos los servidores registran `reflection.Register(srv)` lo que permite usar
`grpcurl` **sin** pasar el archivo `.proto` manualmente.

### Load Balancer gRPC en el Gateway

El Go Gateway implementa un round-robin independiente para los endpoints gRPC:

```go
var grpcRegistry = map[string]*grpcPool{
    "order-service": {addrs: []string{"order-service:9091"}},
    "price-service": {addrs: []string{"price-service:9092"}},
    "user-service":  {addrs: []string{"user-service:9093"}},
}
```

Para escalar un servicio basta con agregar más entradas al slice `addrs` —
el round robin distribuirá las peticiones automáticamente.

---

## Pruebas con grpcurl

> **Requisito:** `grpcurl` instalado — `brew install grpcurl`

### Instalación

```bash
brew install grpcurl
```

### TEST 1 — Listar servicios disponibles (reflection)

```bash
grpcurl -plaintext localhost:9091 list
```

**Resultado:**
```
grpc.reflection.v1.ServerReflection
grpc.reflection.v1alpha.ServerReflection
market.OrderService
```

---

### TEST 2 — Listar métodos de OrderService

```bash
grpcurl -plaintext localhost:9091 list market.OrderService
```

**Resultado:**
```
market.OrderService.CreateOrder
market.OrderService.ListOrders
```

---

### TEST 3 — Listar métodos de PriceService

```bash
grpcurl -plaintext localhost:9092 list market.PriceService
```

**Resultado:**
```
market.PriceService.ListPrices
```

---

### TEST 4 — Listar métodos de UserService

```bash
grpcurl -plaintext localhost:9093 list market.UserService
```

**Resultado:**
```
market.UserService.CreateUser
market.UserService.ListUsers
```

---

### TEST 5 — ListOrders

```bash
grpcurl -plaintext localhost:9091 market.OrderService.ListOrders
```

**Resultado:**
```json
{
  "orders": [
    {
      "id": 1,
      "symbol": "BTC/USD",
      "side": "BUY",
      "price": 64999.63,
      "amount": 0.11,
      "status": "PENDING",
      "createdAt": "2026-09-18T20:40:07Z"
    }
  ]
}
```

---

### TEST 6 — CreateOrder

```bash
grpcurl -plaintext \
  -d '{"symbol":"ETH/USD","side":"BUY","price":3500.0,"amount":2.5}' \
  localhost:9091 market.OrderService.CreateOrder
```

**Resultado:**
```json
{
  "id": 5,
  "symbol": "ETH/USD",
  "side": "BUY",
  "price": 3500,
  "amount": 2.5,
  "status": "PENDING",
  "createdAt": "2026-10-15T00:10:00Z"
}
```

---

### TEST 7 — ListPrices

```bash
grpcurl -plaintext localhost:9092 market.PriceService.ListPrices
```

**Resultado:**
```json
{
  "prices": [
    { "id": 1, "symbol": "SOL/USD", "price": 170.22, "updatedAt": "2026-10-15T00:10:06Z" },
    { "id": 2, "symbol": "BTC/USD", "price": 77653.10, "updatedAt": "2026-10-15T00:10:06Z" },
    { "id": 3, "symbol": "ETH/USD", "price": 4401.40, "updatedAt": "2026-10-15T00:10:06Z" }
  ]
}
```

---

### TEST 8 — ListUsers

```bash
grpcurl -plaintext localhost:9093 market.UserService.ListUsers
```

**Resultado:**
```json
{
  "users": [
    { "id": 1, "name": "Trader Demo", "email": "demo@mercado.com", "balance": 10000, "createdAt": "2026-09-18T20:20:27Z" }
  ]
}
```

---

### TEST 9 — CreateUser

```bash
grpcurl -plaintext \
  -d '{"name":"Alice gRPC","email":"alice@grpc.test","balance":50000.0}' \
  localhost:9093 market.UserService.CreateUser
```

**Resultado:**
```json
{
  "id": 12,
  "name": "Alice gRPC",
  "email": "alice@grpc.test",
  "balance": 50000,
  "createdAt": "2026-10-15T00:10:35Z"
}
```

---

### TEST 10 — Describe schema de un mensaje

```bash
grpcurl -plaintext localhost:9091 describe market.CreateOrderRequest
```

**Resultado:**
```
market.CreateOrderRequest is a message:
message CreateOrderRequest {
  string symbol = 1;
  string side = 2;
  double price = 3;
  double amount = 4;
}
```

---

### TEST 11 — Gateway gRPC-proxy: ListPrices via HTTP

```bash
curl -s http://localhost:8090/grpc/prices
```

**Resultado:**
```json
[
  { "id": 1, "symbol": "SOL/USD", "price": 163.45, "updated_at": "2026-10-15T00:17:03Z" },
  { "id": 2, "symbol": "BTC/USD", "price": 81624.86, "updated_at": "2026-10-15T00:17:03Z" },
  { "id": 3, "symbol": "ETH/USD", "price": 4232.60, "updated_at": "2026-10-15T00:17:03Z" }
]
```
*(Headers respuesta incluyen `X-Transport: grpc` y `X-Served-By: price-service:9092`)*

---

### TEST 12 — Gateway gRPC-proxy: CreateOrder via HTTP

```bash
curl -s -X POST http://localhost:8090/grpc/orders/create \
  -H "Content-Type: application/json" \
  -d '{"symbol":"SOL/USD","side":"SELL","price":163.5,"amount":10}'
```

**Resultado:**
```json
{
  "id": 6,
  "symbol": "SOL/USD",
  "side": "SELL",
  "price": 163.5,
  "amount": 10,
  "status": "PENDING",
  "created_at": "2026-10-15T00:19:37Z"
}
```

---

### TEST 13 — gRPC Status del Gateway

```bash
curl -s http://localhost:8090/grpc/status
```

**Resultado:**
```json
[
  { "service": "order-service", "addrs": ["order-service:9091"] },
  { "service": "price-service", "addrs": ["price-service:9092"] },
  { "service": "user-service",  "addrs": ["user-service:9093"]  }
]
```

---

## Nginx Gateway — configuración

El archivo [`nginx-gateway/nginx.conf`](nginx-gateway/nginx.conf) tiene dos partes:

### 1. Upstream blocks — Load Balancer HTTP

```nginx
upstream order_service {
    server order-service:8081;
}
upstream price_service {
    server price-service:8082;
}
upstream user_service {
    server user-service:8083;
}
```

### 2. Location blocks — Reverse Proxy

```nginx
location /orders {
    proxy_pass http://order_service;
}
location /prices {
    proxy_pass http://price_service;
}
location /users {
    proxy_pass http://user_service;
}
```

---

## Patrones de diseño

| Patrón | Dónde |
|---|---|
| MVC | Cada microservicio (struct + SQLite + handler) |
| API Gateway | nginx-gateway como punto de entrada HTTP |
| API Gateway gRPC | go-gateway como punto de entrada gRPC-proxy |
| Reverse Proxy | `proxy_pass` en nginx.conf |
| Load Balancer Round Robin HTTP | `upstream` blocks en nginx.conf |
| Load Balancer Round Robin gRPC | `grpcPool.next()` en gateway/main.go |
| Database per Service | Cada servicio con su propio SQLite aislado |
| Server Reflection | `reflection.Register` en cada servidor gRPC |

---

## Tecnologías

- **Go 1.22** — microservicios backend + gateway gRPC
- **gRPC + Protobuf** — comunicación binaria entre servicios
- **SQLite** — base de datos por servicio
- **Nginx** — frontend + gateway HTTP/reverse proxy/load balancer
- **Docker + Docker Compose** — contenedores y orquestación
- **Consul** — service discovery
- **grpcurl** — cliente gRPC de línea de comandos para pruebas
