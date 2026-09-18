package main

import (
	"fmt"
	"net/http"
	"simulador-mercado/controllers"
	"simulador-mercado/middleware"
)

// chain aplica una lista de middlewares a un handler.
// El orden importa: el primero en la lista es el más externo (se ejecuta primero).
// Aquí el orden es: Recovery → RequestID → Logger → CORS → handler real
func chain(h http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	// Aplicamos los middlewares de derecha a izquierda
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func main() {
	// Stack de middlewares que se aplica a todas las rutas
	stack := []func(http.HandlerFunc) http.HandlerFunc{
		middleware.Recovery,   // 1. Atrapa panics — siempre primero
		middleware.RequestID,  // 2. Genera el ID de la petición
		middleware.Logger,     // 3. Loguea (ya tiene el RequestID disponible)
		middleware.CORS,       // 4. Agrega headers CORS
	}

	// Rutas
	http.HandleFunc("/", chain(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "views/index.html")
	}, stack...))

	http.HandleFunc("/orders", chain(controllers.GetOrdersHandler, stack...))
	http.HandleFunc("/orders/create", chain(controllers.CreateOrderHandler, stack...))

	fmt.Println("Servidor corriendo en http://localhost:8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Error al iniciar el servidor:", err)
	}
}
