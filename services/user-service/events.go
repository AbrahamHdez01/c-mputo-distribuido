package main

// =============================================================================
// events.go — Capa de mensajería asíncrona con Apache Kafka
//
// Este archivo es IGUAL en los cuatro servicios (misma idea que la carpeta
// proto/: el contrato se copia para que cada servicio sea independiente).
//
// Dos conceptos que conviene no confundir:
//
//   • TÓPICO  → el "canal" donde se publican los eventos. Es append-only:
//               los mensajes no se borran cuando alguien los lee.
//               Cada servicio publica en el tópico que le corresponde.
//
//   • COLA    → en Kafka la cola es el CONSUMER GROUP. Kafka recuerda, por
//               cada grupo, hasta qué mensaje ya leyó (el "offset"). Por eso
//               dos grupos distintos pueden leer el MISMO tópico sin quitarse
//               mensajes: cada grupo tiene su propia cola y su propio avance.
//
// Flujo del sistema:
//
//   order-service  ──publica──→  orders.created   ──→ cola notif-orders
//   order-service  ──publica──→  orders.filled    ──→ cola notif-fills
//   price-service  ──publica──→  prices.updated   ──→ cola notif-prices
//                                                 └─→ cola order-matcher
//   user-service   ──publica──→  users.registered ──→ cola notif-users
// =============================================================================

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ─── Tópicos ─────────────────────────────────────────────────────────────────
// Un tópico por tipo de evento, nombrado como "servicio.que-pasó".
const (
	TopicOrdersCreated   = "orders.created"   // productor: order-service
	TopicOrdersFilled    = "orders.filled"    // productor: order-service (matching)
	TopicPricesUpdated   = "prices.updated"   // productor: price-service
	TopicUsersRegistered = "users.registered" // productor: user-service
)

// ─── Colas (consumer groups) ─────────────────────────────────────────────────
const (
	GroupOrderMatcher = "order-matcher" // order-service        ← prices.updated
	GroupNotifOrders  = "notif-orders"  // notification-service ← orders.created
	GroupNotifFills   = "notif-fills"   // notification-service ← orders.filled
	GroupNotifPrices  = "notif-prices"  // notification-service ← prices.updated
	GroupNotifUsers   = "notif-users"   // notification-service ← users.registered
)

// Event es el "sobre" que viaja por Kafka. Siempre tiene el mismo formato,
// así cualquier consumidor puede leer cualquier tópico sin conocer el detalle.
type Event struct {
	Topic     string          `json:"topic"`
	Service   string          `json:"service"`   // quién lo publicó
	Timestamp time.Time       `json:"timestamp"` // cuándo se publicó
	Payload   json.RawMessage `json:"payload"`   // el dato en sí (orden, precio, usuario)
}

var (
	kafkaWriter *kafka.Writer // productor compartido por todo el servicio
	serviceName string        // nombre que se escribe en Event.Service
)

// brokers devuelve la lista de brokers de Kafka (dentro de Docker: kafka:9092).
func brokers() []string {
	addr := os.Getenv("KAFKA_BROKERS")
	if addr == "" {
		addr = "kafka:9092"
	}
	return strings.Split(addr, ",")
}

// initKafka prepara el productor. No bloquea el arranque del servicio:
// si Kafka todavía no está listo, kafka-go reintenta por su cuenta.
func initKafka(service string) {
	serviceName = service

	kafkaWriter = &kafka.Writer{
		Addr: kafka.TCP(brokers()...),

		// Hash: todos los mensajes con la misma key (ej. "BTC/USD") caen en la
		// misma partición, así se garantiza el ORDEN de los eventos de ese par.
		Balancer: &kafka.Hash{},

		// Esperamos la confirmación del broker antes de seguir: si el mensaje
		// no se guardó, nos enteramos (RequireNone sería "disparar y olvidar").
		RequiredAcks: kafka.RequireOne,

		// Por defecto kafka-go espera 1s para juntar mensajes en un lote.
		// Lo bajamos para que crear una orden no tarde un segundo extra.
		BatchTimeout: 10 * time.Millisecond,

		// Red de seguridad: si kafka-init no alcanzó a crear el tópico,
		// se crea solo con la configuración por defecto del broker.
		AllowAutoTopicCreation: true,
	}

	go waitForKafka()
}

// waitForKafka solo informa en el log cuando el broker ya responde
// (mismo estilo que el registro en Consul).
func waitForKafka() {
	for i := 0; i < 15; i++ {
		conn, err := kafka.Dial("tcp", brokers()[0])
		if err == nil {
			conn.Close()
			log.Printf("[kafka] broker disponible en %s", brokers()[0])
			return
		}
		log.Printf("[kafka] esperando broker... intento %d/15", i+1)
		time.Sleep(2 * time.Second)
	}
	log.Println("[kafka] no se pudo contactar el broker (el servicio sigue funcionando)")
}

// publishEvent envuelve el payload en un Event y lo publica en un tópico.
// La key decide la partición: usamos el símbolo o el email para agrupar.
func publishEvent(topic, key string, payload interface{}) {
	if kafkaWriter == nil {
		return
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[kafka] no se pudo serializar el payload de %s: %v", topic, err)
		return
	}

	body, err := json.Marshal(Event{
		Topic:     topic,
		Service:   serviceName,
		Timestamp: time.Now(),
		Payload:   raw,
	})
	if err != nil {
		log.Printf("[kafka] no se pudo serializar el evento de %s: %v", topic, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	})
	if err != nil {
		// El servicio NO falla si Kafka está caído: así es el desacoplamiento.
		log.Printf("[kafka] error publicando en %s: %v", topic, err)
		return
	}

	log.Printf("[kafka] → publicado en %s (key=%s)", topic, key)
}

// startConsumer arranca una COLA: un consumer group que lee un tópico en
// segundo plano y llama a handle con cada mensaje que llega.
//
// ReadMessage confirma el offset automáticamente, así que si el servicio se
// reinicia, retoma la lectura donde se quedó (no reprocesa todo el tópico).
func startConsumer(group, topic string, handle func(Event)) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers(),
		GroupID:  group, // ← esto es lo que lo convierte en una cola
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
		MaxWait:  500 * time.Millisecond,
	})

	log.Printf("[kafka] cola '%s' suscrita al tópico '%s'", group, topic)

	go func() {
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("[kafka] error leyendo %s: %v", topic, err)
				time.Sleep(2 * time.Second)
				continue
			}

			var ev Event
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("[kafka] mensaje inválido en %s: %v", topic, err)
				continue
			}

			log.Printf("[kafka] ← %s recibió de %s (partición %d, offset %d)",
				group, msg.Topic, msg.Partition, msg.Offset)

			handle(ev)
		}
	}()
}
