package models

import "time"

type Order struct {
	ID        int       `json:"id"`
	Symbol    string    `json:"symbol"` 
	Side      string    `json:"side"`   
	Price     float64   `json:"price"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"` 
	CreatedAt time.Time `json:"created_at"`
}

var OrderDB = []Order{}
var nextID = 1

func CreateOrder(symbol, side string, price, amount float64) Order {
	order := Order{
		ID:        nextID,
		Symbol:    symbol,
		Side:      side,
		Price:     price,
		Amount:    amount,
		Status:    "PENDING",
		CreatedAt: time.Now(),
	}
	nextID++
	OrderDB = append(OrderDB, order)
	return order
}

func GetAllOrders() []Order {
	return OrderDB
}
