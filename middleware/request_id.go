package middleware

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type contextKey string

const RequestIDKey contextKey = "requestID"

// RequestID genera un ID único por petición para poder rastrearla
func RequestID(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(9999))
		ctx := context.WithValue(r.Context(), RequestIDKey, id)
		w.Header().Set("X-Request-ID", id)
		next(w, r.WithContext(ctx))
	}
}

// GetRequestID obtiene el request ID desde el contexto
func GetRequestID(ctx context.Context) string {
	id, ok := ctx.Value(RequestIDKey).(string)
	if !ok {
		return "unknown"
	}
	return id
}
