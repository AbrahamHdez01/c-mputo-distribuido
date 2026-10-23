package main

// =============================================================================
// notification-service — Consumidor de eventos
//
// Este servicio NO tiene lógica de negocio y nadie le llama para que trabaje:
// se queda escuchando los tópicos de Kafka y guarda todo lo que pasa en el
// sistema. Es la prueba visible de que la mensajería asíncrona funciona.
//
// Tiene UNA COLA (consumer group) POR TÓPICO:
//
//   notif-orders  ← orders.created
//   notif-fills   ← orders.filled
//   notif-prices  ← prices.updated
//   notif-users   ← users.registered
// =============================================================================

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// StoredEvent es un evento ya guardado en la bitácora.
type StoredEvent struct {
	ID      int             `json:"id"`
	Topic   string          `json:"topic"`
	Service string          `json:"service"`
	Time    time.Time       `json:"time"`
	Payload json.RawMessage `json:"payload"`
}

var db *sql.DB

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", "/data/events.db")
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		topic      TEXT     NOT NULL,
		service    TEXT     NOT NULL,
		event_time DATETIME NOT NULL,
		payload    TEXT     NOT NULL
	)`)
	if err != nil {
		log.Fatal("Error creando tabla events:", err)
	}

	log.Println("Base de datos de eventos lista.")
}

// ─── Consumidores ─────────────────────────────────────────────────────────────

// saveEvent es el handler que usan las cuatro colas: guarda el evento tal
// como llegó. Al ser el mismo formato (Event), sirve para cualquier tópico.
func saveEvent(ev Event) {
	_, err := db.Exec(
		"INSERT INTO events (topic, service, event_time, payload) VALUES (?, ?, ?, ?)",
		ev.Topic, ev.Service, ev.Timestamp, string(ev.Payload),
	)
	if err != nil {
		log.Printf("[notificaciones] error guardando evento de %s: %v", ev.Topic, err)
		return
	}
	log.Printf("[notificaciones] evento de %s guardado (%s)", ev.Topic, ev.Service)
}

func startConsumers() {
	startConsumer(GroupNotifOrders, TopicOrdersCreated, saveEvent)
	startConsumer(GroupNotifFills, TopicOrdersFilled, saveEvent)
	startConsumer(GroupNotifPrices, TopicPricesUpdated, saveEvent)
	startConsumer(GroupNotifUsers, TopicUsersRegistered, saveEvent)
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

// getEvents devuelve la bitácora, del más nuevo al más viejo.
// Acepta ?topic=prices.updated y ?limit=20
func getEvents(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	var rows *sql.Rows
	var err error

	if topic := r.URL.Query().Get("topic"); topic != "" {
		rows, err = db.Query(`SELECT id, topic, service, event_time, payload FROM events
			WHERE topic = ? ORDER BY id DESC LIMIT ?`, topic, limit)
	} else {
		rows, err = db.Query(`SELECT id, topic, service, event_time, payload FROM events
			ORDER BY id DESC LIMIT ?`, limit)
	}
	if err != nil {
		http.Error(w, "Error leyendo eventos", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	events := []StoredEvent{}
	for rows.Next() {
		var e StoredEvent
		var payload string
		rows.Scan(&e.ID, &e.Topic, &e.Service, &e.Time, &payload)
		e.Payload = json.RawMessage(payload)
		events = append(events, e)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// getStats cuenta cuántos mensajes recibió cada cola. Sirve para demostrar
// de un vistazo que los cuatro tópicos están fluyendo.
func getStats(w http.ResponseWriter, r *http.Request) {
	// Last es string y no time.Time a propósito: SQLite devuelve el resultado
	// de MAX() como texto (pierde el tipo DATETIME de la columna), así que
	// go-sqlite3 ya no lo convierte a fecha.
	type TopicStat struct {
		Topic string `json:"topic"`
		Total int    `json:"total"`
		Last  string `json:"last_event"`
	}

	rows, err := db.Query(`SELECT topic, COUNT(*), MAX(event_time) FROM events
		GROUP BY topic ORDER BY topic`)
	if err != nil {
		http.Error(w, "Error leyendo estadísticas", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	stats := []TopicStat{}
	for rows.Next() {
		var s TopicStat
		rows.Scan(&s.Topic, &s.Total, &s.Last)
		stats = append(stats, s)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// ─── Consul ───────────────────────────────────────────────────────────────────

func registerConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	payload := map[string]interface{}{
		"ID":      "notification-service",
		"Name":    "notification-service",
		"Address": "notification-service",
		"Port":    8084,
		"Check": map[string]string{
			"HTTP":     "http://notification-service:8084/health",
			"Interval": "10s",
			"Timeout":  "3s",
		},
	}
	body, _ := json.Marshal(payload)
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/register", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			log.Println("Registrado en Consul correctamente")
			return
		}
		log.Printf("Esperando Consul... intento %d/10", i+1)
		time.Sleep(3 * time.Second)
	}
	log.Println("No se pudo registrar en Consul (el servicio seguirá funcionando)")
}

func deregisterConsul() {
	consulAddr := os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	req, _ := http.NewRequest(http.MethodPut, "http://"+consulAddr+"/v1/agent/service/deregister/notification-service", nil)
	client := &http.Client{Timeout: 3 * time.Second}
	client.Do(req)
	log.Println("Desregistrado de Consul")
}

func main() {
	initDB()

	// Kafka: aquí vive el consumidor. initKafka también deja listo el
	// productor, aunque este servicio no publica nada.
	initKafka("notification-service")
	startConsumers()

	go registerConsul()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		deregisterConsul()
		os.Exit(0)
	}()

	http.HandleFunc("/events", getEvents)
	http.HandleFunc("/events/stats", getStats)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"status":"ok","service":"notification-service"}`)
	})

	log.Println("notification-service HTTP corriendo en :8084")
	log.Fatal(http.ListenAndServe(":8084", nil))
}
