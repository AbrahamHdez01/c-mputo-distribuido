# Simulador de Mercado — Microservicios

Proyecto de **Cómputo Distribuido**.
Sistema distribuido que simula un mercado financiero usando microservicios con Docker.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────┐
│  USUARIO (navegador)                                      │
└─────────────────────┬────────────────────────────────────┘
                      │ http://localhost:3000
                      ▼
┌──────────────────────────────────────────────────────────┐
│  FRONTEND  (Nginx — puerto 3000)                         │
│  index.html — interfaz web con JS puro                   │
└─────────────────────┬────────────────────────────────────┘
                      │ http://localhost:8080
                      ▼
┌──────────────────────────────────────────────────────────┐
│  NGINX GATEWAY  (Nginx — puerto 8080)                    │
│  • Reverse Proxy                                         │
│  • Load Balancer (upstream blocks)                       │
└──────┬──────────────┬──────────────────┬─────────────────┘
       │              │                  │
       ▼              ▼                  ▼
  ┌──────────┐  ┌───────────┐  ┌─────────────┐
  │  order   │  │   price   │  │    user     │
  │ service  │  │  service  │  │   service   │
  │  :8081   │  │   :8082   │  │    :8083    │
  │  SQLite  │  │   SQLite  │  │   SQLite    │
  └──────────┘  └───────────┘  └─────────────┘
```

---

## Estructura del proyecto

```
simulador-mercado/
├── docker-compose.yml
├── setup.sh
│
├── frontend/                   ← Contenedor 1: UI (Nginx)
│   ├── Dockerfile
│   └── index.html
│
├── nginx-gateway/              ← Contenedor 2: Reverse Proxy + Load Balancer (Nginx)
│   ├── Dockerfile
│   └── nginx.conf
│
└── services/
    ├── order-service/          ← Contenedor 3: Órdenes
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   └── main.go
    ├── price-service/          ← Contenedor 4: Precios
    │   ├── Dockerfile
    │   ├── go.mod / go.sum
    │   └── main.go
    └── user-service/           ← Contenedor 5: Usuarios
        ├── Dockerfile
        ├── go.mod / go.sum
        └── main.go
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
| **Nginx Gateway** | http://localhost:8080 |
| **Health Check** | http://localhost:8080/health |
| **Service Discovery** | http://localhost:8080/services |

Los microservicios solo son accesibles desde dentro de la red Docker (por el gateway).

---

## API — Endpoints (todos via gateway :8080)

| Método | Ruta | Descripción |
|---|---|---|
| GET | /orders | Lista todas las órdenes |
| POST | /orders/create | Crea una orden |
| GET | /prices | Precios actuales |
| GET | /users | Lista traders |
| POST | /users/create | Registra un trader |
| GET | /health | Estado del gateway |
| GET | /services | Servicios registrados |

---

## Nginx Gateway — configuración

El archivo [`nginx-gateway/nginx.conf`](nginx-gateway/nginx.conf) tiene dos partes:

### 1. Upstream blocks — Load Balancer

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

Cada bloque `upstream` define las instancias de un servicio. Para escalar solo hay que agregar más líneas `server`. Nginx distribuye las peticiones en Round Robin por defecto.

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

Cada `location` mapea un path a su upstream. El cliente solo conoce el gateway — nunca las URLs internas de los servicios.

---

## Patrones de diseño

| Patrón | Dónde |
|---|---|
| MVC | Cada microservicio (struct + SQLite + handler HTTP) |
| API Gateway | nginx-gateway como punto de entrada único |
| Reverse Proxy | `proxy_pass` en nginx.conf |
| Load Balancer Round Robin | `upstream` blocks en nginx.conf |
| Database per Service | Cada servicio con su propio SQLite aislado |

---

## Tecnologías

- **Go 1.22** — microservicios backend
- **SQLite** — base de datos por servicio
- **Nginx** — frontend + gateway/reverse proxy/load balancer
- **Docker + Docker Compose** — contenedores y orquestación
