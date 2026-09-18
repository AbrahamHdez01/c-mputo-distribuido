package main

import (
	"fmt"
	"net/http"
	"simulador-mercado/controllers"
	"simulador-mercado/middleware"
)

// chain aplica middlewares a un handler, de afuera hacia adentro
func chain(h http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func main() {
	// middlewares que se usan en todas las rutas
	stack := []func(http.HandlerFunc) http.HandlerFunc{
		middleware.Recovery,
		middleware.RequestID,
		middleware.Logger,
		middleware.CORS,
	}

	// rutas
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
