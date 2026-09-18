package middleware

import (
	"fmt"
	"net/http"
	"time"
)

// Logger imprime en consola información de cada petición:
// método HTTP, ruta, duración y el Request ID.
// Muy útil para ver qué está pasando en el servidor.
func Logger(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Tomamos el Request ID del contexto (lo puso el middleware anterior)
		requestID := GetRequestID(r.Context())

		// Ejecutamos el handler real
		next(w, r)

		// Después de ejecutar, calculamos cuánto tardó
		duration := time.Since(start)

		fmt.Printf("[%s] %s %s — duración: %v — requestID: %s\n",
			time.Now().Format("2006-01-02 15:04:05"),
			r.Method,
			r.URL.Path,
			duration,
			requestID,
		)
	}
}
