package middleware

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// contextKey es un tipo propio para evitar colisiones en el contexto
type contextKey string

const RequestIDKey contextKey = "requestID"

// RequestID genera un ID único para cada petición HTTP.
// Esto es importante en sistemas distribuidos: si la petición
// pasa por varios servicios, todos pueden compartir el mismo ID
// para rastrear qué pasó y en qué orden.
func RequestID(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generamos un ID simple con timestamp + número random
		id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(9999))

		// Lo guardamos en el contexto de la petición
		ctx := context.WithValue(r.Context(), RequestIDKey, id)

		// Lo enviamos también en el header de respuesta
		// para que el cliente pueda verlo
		w.Header().Set("X-Request-ID", id)

		// Llamamos al siguiente handler con el contexto actualizado
		next(w, r.WithContext(ctx))
	}
}

// GetRequestID es un helper para obtener el ID desde el contexto
// en cualquier parte de la app (controllers, models, etc.)
func GetRequestID(ctx context.Context) string {
	id, ok := ctx.Value(RequestIDKey).(string)
	if !ok {
		return "unknown"
	}
	return id
}
