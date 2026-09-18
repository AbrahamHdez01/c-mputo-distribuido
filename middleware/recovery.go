package middleware

import (
	"fmt"
	"net/http"
)

// Recovery atrapa cualquier panic que ocurra dentro de un handler
// y en vez de tirar abajo todo el servidor, devuelve un error 500.
// Sin esto, un solo bug haría caer toda la app.
func Recovery(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// defer + recover = si algo explota, lo atrapamos aquí
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
