# Etapa 1: compilar el binario
FROM golang:1.22-alpine AS builder

# gcc es necesario para go-sqlite3 (usa CGO)
RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o order-service .

# Etapa 2: imagen final mínima
FROM alpine:latest
RUN apk add --no-cache libc6-compat

WORKDIR /app
COPY --from=builder /app/order-service .

# Carpeta donde vivirá la base de datos SQLite
RUN mkdir -p /data

EXPOSE 8081
CMD ["./order-service"]
