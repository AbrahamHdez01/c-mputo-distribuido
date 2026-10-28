# Etapa 1: compilar el binario (Go puro, sin CGO: ya no usamos SQLite)
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o order-service .

# Etapa 2: imagen final mínima
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/order-service .

EXPOSE 8081 9091
CMD ["./order-service"]
