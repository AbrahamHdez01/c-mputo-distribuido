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

### Capa asíncrona (Apache Kafka)

Lo de arriba es **comunicación síncrona**: alguien pide y espera respuesta.
Kafka agrega la **comunicación asíncrona**: el servicio publica un evento y
sigue trabajando; quien le interese lo consume después, por su cuenta.

```
  order-service ──publica──→ [ orders.created ]───→ (notif-orders)  ┐
  order-service ──publica──→ [ orders.filled  ]───→ (notif-fills)   │
  user-service  ──publica──→ [ users.registered]──→ (notif-users)   ├─→ notification-service
                                                                    │      (bitácora
  price-service ──publica──→ [ prices.updated ]───→ (notif-prices)  ┘      de eventos)
                                    │
                                    └────────────→ (order-matcher) ──→ order-service
                                                                        completa órdenes
                                                                        PENDING → FILLED

      [ tópico ]  = canal de mensajes        (nadie los borra al leerlos)
      (cola)      = consumer group de Kafka  (cada uno con su propio avance)
```

El price-service **no sabe que el order-service existe**: solo publica precios.
Y aun así las órdenes se completan solas. Eso es el desacoplamiento.

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
    │   ├── main.go             ← + matching asíncrono (consume prices.updated)
    │   ├── events.go           ← Capa de Kafka (productor + consumidor)
    │   └── proto/
    ├── price-service/          ← Contenedor 5: Precios  (HTTP :8082 + gRPC :9092)
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   ├── main.go             ← + publica cada tick en prices.updated
    │   ├── events.go
    │   └── proto/
    ├── user-service/           ← Contenedor 6: Usuarios (HTTP :8083 + gRPC :9093)
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   ├── main.go             ← + publica en users.registered
    │   ├── events.go
    │   └── proto/
    └── notification-service/   ← Contenedor 7: Consumidor Kafka (HTTP :8084)
        ├── Dockerfile
        ├── go.mod / go.sum
        ├── main.go             ← 4 colas + bitácora de eventos en SQLite
        └── events.go
```

Contenedores de infraestructura que agrega Kafka: `kafka` (broker),
`kafka-init` (crea los tópicos y se apaga) y `kafka-ui` (interfaz web).

> **Nota:** `events.go` es **el mismo archivo copiado** en los cuatro
> servicios, igual que la carpeta `proto/`. Así cada microservicio compila
> solo, sin depender de los demás.

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
| **Bitácora de eventos** | http://localhost:8080/events |
| **Stats por tópico** | http://localhost:8080/events/stats |
| **Kafka UI** | http://localhost:8085 |
| **Kafka (desde el host)** | localhost:9094 |

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
| GET | /events | Bitácora de eventos de Kafka (últimos 100) |
| GET | /events?topic=prices.updated | Filtra por tópico |
| GET | /events?limit=10 | Limita la cantidad |
| GET | /events/stats | Cuántos mensajes consumió cada cola |
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

---

## Apache Kafka — Cola de mensajería asíncrona

### Tópicos vs. colas (lo que más se confunde)

En Kafka **el tópico no es la cola**:

| | Tópico | Cola (consumer group) |
|---|---|---|
| Qué es | El canal donde se publican los mensajes | El grupo que lee ese canal |
| ¿Se borran los mensajes al leerlos? | **No**, el tópico es append-only | No borra nada; solo avanza su *offset* |
| ¿Dos lectores se pelean los mensajes? | — | **No** si son grupos distintos: cada grupo lleva su propio avance |
| Ejemplo aquí | `prices.updated` | `order-matcher` y `notif-prices` |

Por eso `prices.updated` lo leen **dos colas al mismo tiempo** y las dos reciben
todos los mensajes: `order-matcher` para completar órdenes y `notif-prices`
para guardarlos en la bitácora. Si en cambio levantáramos **dos instancias del
mismo grupo**, entonces sí se repartirían los mensajes entre ellas (una por
partición) — eso es escalar un consumidor.

### Tópicos implementados

Cada servicio publica en el tópico que le corresponde:

| Tópico | Productor | Cuándo se publica | Key (define la partición) |
|---|---|---|---|
| `orders.created` | order-service | Al crear una orden (por HTTP y por gRPC) | símbolo (`BTC/USD`) |
| `orders.filled` | order-service | Cuando el matching completa una orden | símbolo |
| `prices.updated` | price-service | Cada 5 s, un mensaje por par de monedas | símbolo |
| `users.registered` | user-service | Al registrar un trader | email |

Los cuatro se crean con **3 particiones** y factor de replicación 1
(un solo broker). La *key* decide la partición: todos los eventos de `BTC/USD`
caen en la misma, así que **nunca se desordenan entre sí**.

### Colas implementadas (consumer groups)

| Cola | Servicio que la corre | Tópico que consume | Qué hace con el mensaje |
|---|---|---|---|
| `order-matcher` | order-service | `prices.updated` | Completa órdenes PENDING → FILLED |
| `notif-orders` | notification-service | `orders.created` | La guarda en la bitácora |
| `notif-fills` | notification-service | `orders.filled` | La guarda en la bitácora |
| `notif-prices` | notification-service | `prices.updated` | La guarda en la bitácora |
| `notif-users` | notification-service | `users.registered` | La guarda en la bitácora |

### El formato de los mensajes

Todos los mensajes usan el mismo "sobre", así cualquier consumidor puede leer
cualquier tópico sin conocer el detalle:

```json
{
  "topic": "orders.created",
  "service": "order-service",
  "timestamp": "2026-10-23T22:14:03.512Z",
  "payload": {
    "id": 7,
    "symbol": "BTC/USD",
    "side": "SELL",
    "price": 65200.0,
    "amount": 0.5,
    "status": "PENDING",
    "created_at": "2026-10-23T22:14:03Z"
  }
}
```

### La implementación — `events.go`

El archivo está copiado igual en los 4 servicios (misma idea que `proto/`).
Tiene tres piezas:

**1. El productor** — uno por servicio, compartido por todos los handlers:

```go
kafkaWriter = &kafka.Writer{
    Addr:         kafka.TCP(brokers()...),
    Balancer:     &kafka.Hash{},        // misma key → misma partición → orden garantizado
    RequiredAcks: kafka.RequireOne,     // esperamos la confirmación del broker
    BatchTimeout: 10 * time.Millisecond,
    AllowAutoTopicCreation: true,       // red de seguridad
}
```

**2. Publicar** — envuelve el dato en un `Event` y lo manda:

```go
publishEvent(TopicOrdersCreated, order.Symbol, order)
```

Si Kafka está caído, `publishEvent` solo escribe el error en el log: **el
servicio no falla**. Crear una orden sigue funcionando. Eso es justamente la
ventaja de la mensajería asíncrona sobre una llamada directa.

**3. Consumir** — una función arranca una cola en segundo plano:

```go
startConsumer(GroupOrderMatcher, TopicPricesUpdated, onPriceUpdated)
```

`ReadMessage` confirma el offset automáticamente, así que si el contenedor se
reinicia **retoma la lectura donde se quedó** en vez de reprocesar todo.

### El matching asíncrono (la parte interesante)

El `order-service` es productor **y** consumidor. Consume `prices.updated` y
completa las órdenes que el mercado ya alcanzó:

```go
// una BUY  se completa si el mercado bajó hasta su precio o menos
// una SELL se completa si el mercado subió hasta su precio o más
UPDATE orders SET status = 'FILLED'
 WHERE status = 'PENDING' AND symbol = ?
   AND ((side = 'BUY' AND price >= ?) OR (side = 'SELL' AND price <= ?))
```

Nadie llama a esta función: se dispara sola con cada mensaje de Kafka. Y cuando
completa una orden, **publica un evento nuevo** (`orders.filled`), que a su vez
otra cola consume. Esa es la cadena de eventos.

---

## Pruebas de Kafka

### TEST 14 — Ver los tópicos creados

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Resultado:**
```
__consumer_offsets
orders.created
orders.filled
prices.updated
users.registered
```

> `__consumer_offsets` es un tópico interno de Kafka: ahí guarda el avance
> (offset) de cada cola. No lo creamos nosotros.

---

### TEST 15 — Describir un tópico (particiones y réplicas)

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic prices.updated
```

**Resultado:**
```
Topic: prices.updated  PartitionCount: 3  ReplicationFactor: 1
  Topic: prices.updated  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: prices.updated  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: prices.updated  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

---

### TEST 16 — Ver los mensajes en vivo (console consumer)

Deja esto corriendo y verás un mensaje nuevo cada 5 segundos:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prices.updated --from-beginning
```

**Resultado:**
```json
{"topic":"prices.updated","service":"price-service","timestamp":"2026-10-23T23:05:25.682704886Z","payload":{"price":166.5901876289965,"symbol":"SOL/USD","updated_at":"2026-10-23T23:05:25.678648678Z"}}
{"topic":"prices.updated","service":"price-service","timestamp":"2026-10-23T23:05:25.704284803Z","payload":{"price":78494.31950636879,"symbol":"BTC/USD","updated_at":"2026-10-23T23:05:25.697233928Z"}}
{"topic":"prices.updated","service":"price-service","timestamp":"2026-10-23T23:05:25.721194511Z","payload":{"price":4248.318684893078,"symbol":"ETH/USD","updated_at":"2026-10-23T23:05:25.716527803Z"}}
```

Y un mensaje de `orders.created`:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created --from-beginning --max-messages 1
```

```json
{"topic":"orders.created","service":"order-service","timestamp":"2026-10-23T22:58:25.535073511Z","payload":{"id":7,"symbol":"BTC/USD","side":"SELL","price":82016.62,"amount":0.5,"status":"PENDING","created_at":"2026-10-23T22:58:25.531788803Z"}}
```

> Sal con `Ctrl+C`. Ojo: este comando crea su propia cola temporal, así que
> **no le quita mensajes** a `order-matcher` ni a `notif-prices`.

---

### TEST 17 — Ver las colas y su retraso (lag)

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
```

**Resultado:**
```
order-matcher
notif-orders
notif-fills
notif-prices
notif-users
```

Y el detalle de una cola:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-matcher
```

**Resultado** (recortado: el comando también imprime CONSUMER-ID, HOST y CLIENT-ID):
```
GROUP          TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-matcher  prices.updated  0          82              82              0
order-matcher  prices.updated  1          82              82              0
order-matcher  prices.updated  2          82              82              0
```

El CLIENT-ID confirma quién es el consumidor:
`order-service@29110d75d145 (github.com/segmentio/kafka-go)`

`LAG 0` significa que la cola va al día. Si apagas el order-service un rato y
lo vuelves a prender, verás el LAG subir y luego bajar: **los mensajes se
quedaron esperando en el tópico**. Eso es lo que hace una cola de mensajería.

---

### TEST 18 — La bitácora de eventos (consumidor en acción)

```bash
curl -s http://localhost:8080/events/stats
```

**Resultado:**
```json
[
  { "topic": "orders.created",   "total": 3,   "last_event": "2026-10-23 23:03:03.535097596+00:00" },
  { "topic": "orders.filled",    "total": 4,   "last_event": "2026-10-23 23:03:04.874208589+00:00" },
  { "topic": "prices.updated",   "total": 186, "last_event": "2026-10-23 23:03:04.923938425+00:00" },
  { "topic": "users.registered", "total": 1,   "last_event": "2026-10-23 23:03:02.887322375+00:00" }
]
```

> Un tópico solo aparece aquí cuando ya recibió al menos un mensaje. Si acabas
> de levantar el sistema, `users.registered` no saldrá hasta que registres un
> trader.

Y los últimos eventos de un tópico:

```bash
curl -s "http://localhost:8080/events?topic=orders.created&limit=2"
```

**Resultado:**
```json
[
  {
    "id": 179,
    "topic": "orders.created",
    "service": "order-service",
    "time": "2026-10-23T23:02:29.535097596Z",
    "payload": { "id": 8, "symbol": "BTC/USD", "side": "SELL", "price": 79859.01, "amount": 0.5, "status": "PENDING", "created_at": "2026-10-23T23:02:29.531788803Z" }
  }
]
```

---

### TEST 19 — Demostrar el flujo asíncrono completo

Este es **el test que prueba que la cola funciona de punta a punta**.

1. Mira el precio actual de BTC:

```bash
curl -s http://localhost:8080/prices
```

2. Crea una orden SELL **un poco por debajo** del precio de mercado. Al estar
   debajo, el mercado ya la alcanzó, así que el matching la completará en el
   próximo mensaje de Kafka (≤ 5 s):

```bash
# si el mercado está en 79938, usa algo como 79859
curl -s -X POST http://localhost:8080/orders/create \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTC/USD","side":"SELL","price":79859.01,"amount":0.5}'
```

**Resultado:**
```json
{"id":8,"symbol":"BTC/USD","side":"SELL","price":79859.01,"amount":0.5,"status":"PENDING","created_at":"..."}
```

Fíjate bien: el gateway respondió **`PENDING`**. El matching **no ocurre
dentro de la petición HTTP** — ese es justamente el punto.

3. Espera unos segundos **sin hacer nada** y vuelve a consultar:

```bash
curl -s http://localhost:8080/orders
```

**Resultado:** la orden ahora dice `"status": "FILLED"`. Nadie llamó a ningún
endpoint para cambiarla: la completó la cola `order-matcher` al recibir un
precio nuevo por Kafka.

4. Compruébalo en el log:

```bash
docker compose logs order-service | grep -E "matching|orders.filled"
```

**Resultado:**
```
order-service | [kafka] ← order-matcher recibió de prices.updated (partición 1, offset 72)
order-service | [matching] orden #8 SELL BTC/USD @ 79859.01 completada (mercado 79945.16)
order-service | [kafka] → publicado en orders.filled (key=BTC/USD)
```

Y la cadena sigue: ese `orders.filled` lo recibe la cola `notif-fills`:

```bash
docker compose logs notification-service | grep notif-fills
```

```
notification-service | [kafka] ← notif-fills recibió de orders.filled (partición 1, offset 0)
notification-service | [notificaciones] evento de orders.filled guardado (order-service)
```

> **Variante: una orden que se queda esperando.** Si en el paso 2 pones el
> precio **arriba** del mercado (ej. 0.15 % más), la orden se queda en PENDING
> hasta que el mercado suba lo suficiente. Como el precio se mueve al azar
> (±0.5 % por tick), eso puede tardar minutos o no pasar en un buen rato: es
> una orden en reposo, igual que en un mercado real.

---

### TEST 20 — Kafka UI (interfaz web)

Abre **http://localhost:8085**

Ahí se ve, sin usar la terminal:

- **Topics** → los 4 tópicos, con sus particiones y cuántos mensajes tiene cada uno
- **Topics → prices.updated → Messages** → el contenido JSON de cada mensaje
- **Consumers** → las 5 colas, qué tópico lee cada una y su LAG en tiempo real

Es la forma más clara de ver, de un vistazo, que los tópicos y las colas están
funcionando.

---

### TEST 21 — Qué pasa si Kafka se cae

Esto demuestra la ventaja real de la mensajería asíncrona frente a una llamada
directa entre servicios.

1. Apaga el broker:

```bash
docker compose stop kafka
```

2. Crea una orden con Kafka caído:

```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8080/orders/create \
  -H "Content-Type: application/json" \
  -d '{"symbol":"SOL/USD","side":"BUY","price":200,"amount":1}'
```

**Resultado:**
```json
{"id":10,"symbol":"SOL/USD","side":"BUY","price":200,"amount":1,"status":"PENDING","created_at":"..."}
HTTP 201
```

**El servicio siguió funcionando.** Las lecturas (`/orders`, `/prices`) y los
endpoints gRPC (`/grpc/users`) también responden 200. Lo único que pasa es un
error en el log:

```
[kafka] error publicando en orders.created: Kafka write errors (1/1), errors:
  [kafka.(*Client).Produce: dial tcp: lookup kafka on 127.0.0.11:53: no such host]
```

Si en vez de Kafka el order-service llamara al price-service por HTTP y ese
estuviera caído, la petición del usuario **fallaría**. Con la cola, no.

3. Préndelo otra vez:

```bash
docker compose start kafka
```

Los consumidores **se reconectan solos** (kafka-go reintenta), consumen lo que
se acumuló y el LAG vuelve a 0. La orden #10 se completa en cuanto llega el
primer precio nuevo:

```
[matching] orden #10 BUY SOL/USD @ 200.00 completada (mercado 168.70)
```

Mientras los consumidores se reconectan se puede ver el LAG subir:

```
notif-prices  prices.updated  0  67  72  LAG=5
```

y bajar a 0 unos segundos después. **Ese LAG es la cola haciendo su trabajo:**
los mensajes esperaron en el tópico hasta que hubo quién los leyera.

> **Límite honesto de esta implementación:** el evento que no se pudo publicar
> mientras Kafka estaba caído **se perdió** (la orden sí quedó guardada en
> SQLite, pero su `orders.created` nunca llegó a la bitácora). Para no perderlo
> habría que guardar el evento en la misma base de datos junto con la orden y
> publicarlo después — eso es el patrón **Transactional Outbox**, que queda
> fuera del alcance de esta práctica.

---

### Notas de implementación

- **El broker es efímero:** no le pusimos volumen, así que `docker compose down`
  borra los mensajes. La bitácora del notification-service **sí persiste**
  (tiene su volumen `notification-data`). Para que Kafka también persista
  bastaría agregarle un volumen en `/var/lib/kafka/data` y apuntar ahí
  `KAFKA_LOG_DIRS`.
- **Orden de arranque:** los servicios tienen
  `depends_on: kafka-init: service_completed_successfully`, así que Docker no
  los levanta hasta que los 4 tópicos existen. Por eso el primer
  `docker compose up` tarda más: espera el healthcheck de Kafka.
- **Si Kafka se cae**, los microservicios siguen respondiendo HTTP y gRPC con
  normalidad; solo dejan de publicar eventos (y lo dicen en el log).
  Ver el TEST 21.
- **El `prices.updated` genera muchos eventos** (3 mensajes cada 5 s, ~2,160
  por hora). Por eso `/events` devuelve solo los últimos 100 por defecto y
  acepta `?topic=` para filtrar.
- **Por qué `segmentio/kafka-go` y no el cliente de Confluent:** kafka-go es Go
  puro. El de Confluent necesita la librería de C `librdkafka`, que habría
  obligado a cambiar los Dockerfiles de Alpine.

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
| Publish / Subscribe | Tópicos de Kafka en `events.go` |
| Message Queue | Consumer groups (`order-matcher`, `notif-*`) |
| Event-Driven Architecture | El matching se dispara por un evento, no por una petición |
| Event Log / Bitácora | notification-service guarda todos los eventos |
| Competing Consumers | 3 particiones por tópico: se puede escalar el consumidor |

---

## Tecnologías

- **Go 1.22** — microservicios backend + gateway gRPC
- **gRPC + Protobuf** — comunicación binaria síncrona entre servicios
- **Apache Kafka 3.7 (modo KRaft)** — cola de mensajería asíncrona
- **segmentio/kafka-go** — cliente de Kafka en Go puro (sin dependencias de C)
- **Kafka UI** — interfaz web para ver tópicos, mensajes y colas
- **SQLite** — base de datos por servicio
- **Nginx** — frontend + gateway HTTP/reverse proxy/load balancer
- **Docker + Docker Compose** — contenedores y orquestación
- **Consul** — service discovery
- **grpcurl** — cliente gRPC de línea de comandos para pruebas
- **kafka-topics.sh / kafka-console-consumer.sh** — CLI de Kafka para pruebas
