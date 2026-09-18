package middleware

import (
	"fmt"
	"net/http"
	"time"
)

// Logger imprime método, ruta, duración y request ID de cada petición
func Logger(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := GetRequestID(r.Context())

		next(w, r)

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
