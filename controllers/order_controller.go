package controllers

import (
	"encoding/json"
	"net/http"
	"simulador-mercado/models"
	"strconv"
)

func GetOrdersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	orders := models.GetAllOrders()
	json.NewEncoder(w).Encode(orders)
}

func CreateOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	symbol := r.FormValue("symbol")
	side := r.FormValue("side")
	price, _ := strconv.ParseFloat(r.FormValue("price"), 64)
	amount, _ := strconv.ParseFloat(r.FormValue("amount"), 64)

	newOrder := models.CreateOrder(symbol, side, price, amount)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newOrder)
}
