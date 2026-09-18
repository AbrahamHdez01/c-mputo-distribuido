package middleware

import "net/http"

// CORS agrega los headers necesarios para que un frontend
// (React, Vue, etc.) en otro puerto/dominio pueda hacer peticiones
// a esta API sin que el navegador las bloquee.
func CORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Permitimos cualquier origen (en producción esto sería más estricto)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")

		// Las peticiones OPTIONS son "preflight" del navegador,
		// solo hay que responderlas con 200 y ya.
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}
