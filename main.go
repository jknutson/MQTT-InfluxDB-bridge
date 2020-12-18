package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb1-client/v2"

	// TODO: use native flag library
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerURL string
var dbURL string

func sendValueToDatabase(database string, source string, sensor string, value float64) {
	logFields := fmt.Sprintf("database=%s source=%s sensor=%s", database, source, sensor)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: dbURL,
	})
	if err != nil {
		log.Fatal(err, logFields)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  database,
		Precision: "s", // TODO: does this precision make sense for temp/mV?
	})
	if err != nil {
		log.Fatal(err, logFields)
	}

	// Create a point and add to batch
	// TODO: get unit (below) as a param
	tags := map[string]string{"source": source, "unit": "fahrenheit"}

	fields := map[string]interface{}{
		"value": value,
	}

	pt, err := client.NewPoint(sensor, tags, fields, time.Now())
	if err != nil {
		log.Fatal(err, logFields)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Fatal(err, logFields)
	}

}

func receiveMQTTMessage(ctx context.Context, receiveChannel <-chan MQTT.Message) {
	for {
		select {
		case <-ctx.Done():
			log.Print("Context cancelled, quitting listener...")
			return
		case message := <-receiveChannel:
			logFields := fmt.Sprintf("topic=%s payload=%s", message.Topic(), string(message.Payload()))

			log.Print("Received message", logFields)

			topicParts := strings.Split(message.Topic(), "/")

			if len(topicParts) >= 3 {
				// TODO: check out if the "sensor" is stdout/stderr/lastwill
				database := topicParts[0]
				source := topicParts[1]
				sensor := topicParts[2]

				payload, err := strconv.ParseFloat(string(message.Payload()), 32)

				if err != nil {
					log.Printf("Failed to parse number for payload, error: %s, %s", err, logFields)
					continue
				}
				sendValueToDatabase(database, source, sensor, payload)
			} else {
				log.Print("Message did not contain expected topic parts, skipping it", logFields)
			}
		}
	}
}

func createMQTTClient(brokerURL string, channel chan<- MQTT.Message) {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("go-server")

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		channel <- msg
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
}

func main() {
	readConfiguration()

	receiveChannel := make(chan MQTT.Message)

	createMQTTClient(brokerURL, receiveChannel)

	var wg sync.WaitGroup

	// Handle interrupt and term signals gracefully
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		select {
		case <-sigs:
			cancel()
		case <-ctx.Done():
		}
		wg.Done()
	}()

	wg.Add(1)
	// start listening to input channel
	go func(ctx context.Context, channel chan MQTT.Message) {
		log.Print("Starting listener")
		defer wg.Done()
		receiveMQTTMessage(ctx, channel)
		log.Print("Listener returned")
	}(ctx, receiveChannel)

	log.Printf("Listener running")
	wg.Wait()
}

func readConfiguration() {
	// allow setting values also with commandline flags
	pflag.String("broker", "tcp://localhost:1883", "MQTT-broker url")
	err := viper.BindPFlag("broker", pflag.Lookup("broker"))
	if err != nil {
		log.Fatal(err)
	}

	pflag.String("db", "http://localhost:8086", "Influx database connection url")
	err = viper.BindPFlag("db", pflag.Lookup("db"))
	if err != nil {
		log.Fatal(err)
	}

	// parse values from environment variables
	viper.AutomaticEnv()

	brokerURL = viper.GetString("broker")
	dbURL = viper.GetString("db")

	log.Printf("Using broker %s", brokerURL)
	log.Printf("Using database %s", dbURL)
}
