package middleware

import (
	"fmt"
	"net/http"
)

// Recovery atrapa panics y devuelve 500 en lugar de caerse
func Recovery(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				requestID := GetRequestID(r.Context())
				fmt.Printf("[PANIC] requestID: %s — error: %v\n", requestID, err)
				http.Error(w, "Error interno del servidor", http.StatusInternalServerError)
			}
		}()

		next(w, r)
	}
}
