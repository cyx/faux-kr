package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/heroku/standin/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/heroku/standin/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/heroku/standin/Godeps/_workspace/src/github.com/rcrowley/go-metrics"
	"github.com/heroku/standin/Godeps/_workspace/src/github.com/rcrowley/go-metrics/librato"
)

var (
	successCounter = metrics.GetOrRegisterCounter("kafka-rest-go.success", metrics.DefaultRegistry)
	errorsCounter  = metrics.GetOrRegisterCounter("kafka-rest-go.errors", metrics.DefaultRegistry)
)

type Payload struct {
	Records []Record `json:"records"`
}

type Record struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Partition int32  `json:"partition,omitempty"`
}

var (
	producer sarama.AsyncProducer
	wg       sync.WaitGroup
)

func getint(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		v, err := strconv.Atoi(v)
		if err != nil {
			return fallback
		}
		return v
	}
	return fallback
}

func main() {
	var addrs []string
	var err error

	for _, u := range strings.Split(os.Getenv("HEROKU_KAFKA_URL"), ",") {
		if u, err := url.Parse(u); err == nil {
			addrs = append(addrs, u.Host)
		}
	}

	conf := sarama.NewConfig()
	conf.Producer.Flush.Messages = getint("PRODUCER_FLUSH_MESSAGES", 1500)
	conf.Producer.Return.Successes = true
	conf.Producer.Flush.Frequency = time.Millisecond * 500

	producer, err = sarama.NewAsyncProducer(addrs, conf)
	if err != nil {
		log.Fatal(err)
	}
	sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)

	wg.Add(2)
	go countSuccess()
	go countErrors()

	if os.Getenv("LIBRATO_TOKEN") != "" {
		go librato.Librato(
			metrics.DefaultRegistry,
			20*time.Second,
			os.Getenv("LIBRATO_OWNER"),
			os.Getenv("LIBRATO_TOKEN"),
			fmt.Sprintf("%s.%s", os.Getenv("LIBRATO_SOURCE"), os.Getenv("DYNO")),
			[]float64{0.50, 0.95, 0.99},
			time.Millisecond,
		)
	}

	mux := httprouter.New()
	mux.POST("/topics/:topic", Post)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), mux))
	wg.Wait()
}

func Post(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	payload := Payload{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("ERR: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	queue(ps.ByName("topic"), payload)
	w.WriteHeader(http.StatusOK)
}

func queue(topic string, p Payload) {
	for _, record := range p.Records {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.StringEncoder(record.Value),
			Key:       sarama.StringEncoder(record.Key),
			Partition: record.Partition,
		}
		producer.Input() <- msg
	}
}

func countSuccess() {
	defer wg.Done()
	for range producer.Successes() {
		successCounter.Inc(1)
	}
}

func countErrors() {
	defer wg.Done()
	for err := range producer.Errors() {
		errorsCounter.Inc(1)
	}
}
