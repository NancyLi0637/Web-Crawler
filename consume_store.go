package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/olivere/elastic.v7"
)

var msgs []string

//create a mapping for es data storing
const mapping = `
{
    "mappings": {
        "properties": {
            "id": {
                "type": "long"
            },
            "title": {
                "type": "text"
            },
            "source": {
                "type": "keyword"
            }
        }
    }
}`

func checkError(msg string, err error) {
	if err != nil {
		fmt.Println(msg, err)
		os.Exit(1)
	}
}

//ConsumeMsgFromKafka consumes messages in the kafka brokers with distinct topics
func ConsumeMsgFromKafka(topic string) {
	//initiate a new consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "myNews",
		"auto.offset.reset": "earliest",
	})

	checkError("Failed to create consumer: %s\n", err)

	//controls topic fetched
	consumer.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)
	//connect to redis
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
	checkError("Failed to connect to redis", err)

	for {
		//consumer poll message
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			value := string(msg.Value)
			ProcessMsg(value, conn)
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, value)
		}
	}

	consumer.Close()
	conn.Close()
}

//ProcessMsg 使用 redis 对新闻内容实现了去重
func ProcessMsg(msg string, conn redis.Conn) {
	fmt.Println("checking")
	data := md5.Sum([]byte(msg))    //convert according to md5
	keyP := fmt.Sprintf("%x", data) //key to be set in redis

	ret, err := conn.Do("EXISTS", keyP) //check if [keyP] exists
	checkError("Failed to check key", err)

	if exist, ok := ret.(int64); ok && exist == 1 {
		return
	}

	_, err = conn.Do("SET", keyP, "")
	checkError("Failed to set key "+keyP, err)

	fmt.Println(redis.Strings(conn.Do("KEYS", "*")))

	msgs = append(msgs, msg) //add msg into msgs

	return
}

//StoreInES stores data in elasticsearch
func StoreInES(indexName string) {

	/////initiate a new es client/////
	ctx := context.Background()                                                                                      //create a context object for API calls
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL([]string{"http://localhost:9200/"}...)) //instantiate a new es client object instance
	checkError("Failed to initiate elasticsearch client", err)

	exists, err := client.IndexExists(indexName).Do(ctx) //check whether given index exists
	checkError("Index name already exists", err)
	if !exists {
		_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx) //create if non-existent
		checkError("Failed to create index", err)
	}

	/////writes in data/////
	data := newsSummary["WYNews"]
	for id, content := range data {
		doc, _ := client.Index().
			Index(indexName).     //writes in index name
			Id(strconv.Itoa(id)). //writes in id
			BodyJson(content).    //writes in data content
			Refresh("wait_for").
			Do(ctx) //executes
		fmt.Printf("Indexed with id=%v, type=%s\n", doc.Id, doc.Type)
		/////check for successful store/////
		result, err := client.Get().
			Index(indexName).
			Id(strconv.Itoa(id)).
			Do(ctx)
		if err != nil {
			panic(err)
		}
		if result.Found {
			fmt.Printf("Got document %v (version=%d, index=%s, type=%s)\n",
				result.Id, result.Version, result.Index, result.Type)
			err := json.Unmarshal(result.Source, &content)
			if err != nil {
				panic(err)
			}
			fmt.Println(id, content.Source, content.Title)
		}
	}

}

func connect(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	go ConsumeMsgFromKafka("WYNews")
	go StoreInES("wy_news2")

}

func main() {
	http.HandleFunc("/consumemsg", connect)
	err := http.ListenAndServe(":9080", nil)
	checkError("Failed to connect to server", err)

}
