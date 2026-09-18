# Etapa 1: compilar
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o price-service .

# Etapa 2: imagen final
FROM alpine:latest
RUN apk add --no-cache libc6-compat

WORKDIR /app
COPY --from=builder /app/price-service .
RUN mkdir -p /data

EXPOSE 8082
CMD ["./price-service"]
