package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var newsSummary map[string][]string //stores the summary of each news API - string KEY:VALUE = news API:news data in json
var contentList map[string][]string //stores the content of each news from news API - string KEY:[]string VALUE = news API: content of each news from API

//create a mapping for es data storing
const mapping = `
	{
		"mappings": {
			"properties": {
				"ctime": {
					"type": "text"
				},
				"title": {
					"type": "text"
				},
				"description": {
					"type": "text"
				}
				"url": {
					"type": "text"
				}
				"source": {
					"type": "text"
				}
			}
		}
	}`

/////数据归一化/////

//ZongHeNews contains a Newslist from its API
type ZongHeNews struct {
	Newslist []ZHNews `json:"newslist"` //获取“newslist”里的news(接口format)
}

//ZHNews formats API data into standard json format
type ZHNews struct {
	Timestamp   string `json:"ctime"` //转换news格式到规定格式
	Title       string `json:"title"`
	Description string `json:"description"`
	URL         string `json:"url"`
	// no source
}

//TouTiaoNews contains a Newslist from its API
type TouTiaoNews struct {
	NewsList []TTNews `json:"newslist"`
}

//TTNews formats API data into standard json format
type TTNews struct {
	ZHNews        //继承ZHNews相同的格式
	Source string `json:"source"`
}

/////数据接入/////

//GetZongHeNews set-up in [newsSummary] and [contentList]
func GetZongHeNews() {
	url := "http://api.tianapi.com/generalnews/index?key=d9455e812137a7cd7c9ab10229c34ec6" //ZHNews url
	resp, err := http.Get(url)                                                             //接口接入、get返回示例

	if err != nil {
		fmt.Printf("Couldn't fetch news", err)
	}

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
	resp.Body.Close()                      //断开连接

	if err != nil {
		fmt.Printf("Couldn't read news", err)
	}

	/////将转换好的标准格式储存到新闻大纲[newsSummary]里面/////
	var zongheNews ZongHeNews           //创造object
	GetStandardFormat(body, zongheNews) //将json格式转换成标准格式
	newsSummary["ZHNews"] = zongheNews  //store in [newsSummary] with key = "ZHNews"

	/////将各个news内容提取出来储存到[contentList]里面/////
	newsContent := GetNewsContent(string(body)) //将接口返回数据转成string方便news url的提取
	contentList["ZHNews"] = newsContent           //store in contentList with key = "ZHNews"

}

//GetTouTiaoNews set-up in [newsSummary] and [contentList]
func GetTouTiaoNews() {
	url := "http://api.tianapi.com/topnews/index?key=d9455e812137a7cd7c9ab10229c34ec6"
	resp, err := http.Get(url) //接口接入、get返回示例

	if err != nil {
		fmt.Printf("Couldn't fetch news", err)
	}

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式
	resp.Body.Close()                      //断开连接

	if err != nil {
		fmt.Printf("Couldn't read news", err)
	}

	/////将转换好的标准格式储存到新闻大纲[newsSummary]里面/////
	var toutiaoNews TouTiaoNews           //创造object
	GetStandardFormat(body, toutiaoNews) //将json格式转换成标准格式
	newsSummary["TTNews"] = toutiaoNews  //store in [newsSummary] with key = "TTNews"

	/////将各个news内容提取出来储存到[contentList]里面/////
	newsContent := GetNewsContent(string(body)) //将接口返回数据转成string方便news url的提取
	contentList["TTNews"] = newsContent           //store in [contentList] with key = "TTNews"
}

//GetStandardFormat changes API data format into standard json format
func GetStandardFormat([]byte body, news) {
	err = json.Unmarshal([]byte(body), &news)
	if err != nil {
		fmt.Println("Couldn't unmarshal json", err)
	}
}

//GetNewsContent returns all the chinese content extracted in a list
func GetNewsContent(string body) []string {
	var newsContent []string
	urlChannel := make(chan string, 200) //create a url channel for storing news urls

	/////set up urls/////
	go URLSetUp(body)

	/////access every url in the channel for article content/////
	for url := range urlChannel {
		fmt.Println("Accessing a new url.\n")
		content := GetArticleContent(url) 			//extract content
		newsContent = append(newsContent, content)  //put into list newsContent
	}

	return newsContent
}

//URLSetUp sets up news urls into [urlChannel]
func URLSetUp(string body) {
	//check for proper function run
	defer func() {
		if r := recover(); r != nil {
			log.Println("[E]", r)
		}
	}()

	urlRegExp := regexp.MustCompile("'url(.*?)html'") //正则匹配url格式 //cant use "http" cuz might include pic url

	urlTag := urlRegExp.FindAllString(body, -1) //get all the news url from API interface
	for _, url := range urlTag {
		croppedURL := url[strings.Index(url, "h") : len(url)-1] //提取“https...html"网址
		urlChannel <- croppedURL                                //put url tag into url channel for re access
	}
}

//GetArticleContent extracts all the chinese content in string form
func GetArticleContent(string url) string {
	chineseRegExp := regexp.MustCompile("[\\p{Han}]+") //正则匹配中文格式

	resp, _ := http.Get(url) //access news article through url
	body := resp.Body        //get html body
	bodyRead, _ := ioutil.ReadAll(body)
	body.Close()

	respStr := string(bodyRead)                                //change html into string
	chineseContent := chineseRegExp.FindAllString(respStr, -1) //find all the chinese content

	return chineseContent
}


/////kafka数据发送与接收/////

//DeliverMsgToKafka sends message to brokers with distinct topics
func DeliverMsgToKafka(string topic) {
	//access data stored in [newsContent]
	data := newsContent[topic]

	//initiate a new producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"boostrap.servers": "127.0.0.1:9092"})

	//check for successful creation of producer before proceeding
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err) //error message
		os.Exit(1)
	}

	/////WRITES MESSAGE/////

	//produce message
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(data)},
		nil)

	//writes message in log
	log.Println("Produced message, waiting for delivery response.")

	/////CHECK MESSAGE DELIVERY/////

	//check delivery reponse with another goroutine
	go func() {
		//event used to listen for the result of send
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivery message to tpic %s [%d] at offset %v\n",
						ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	//wait for message deliveries before shutting down
	producer.Flush(15 * 1000)

}

//ConsumeMsgFromKafka consumes messages in the kafka brokers with distinct topics
func ConsumeMsgFromKafka(topics []string) {
	//initiate a new consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"auto.offset.reset": "smallest",
	})

	//controls topic fetched
	err = consumer.SubscribeTopics(topics, nil)

	for run == true {
		//consumer poll message
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}else {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		}
	}

	consumer.close()
}

func StoreInES(string indexName)(bool){

	/////initiate a new es client/////
	ctx := context.Background() //create a context object for API calls
	client, err := elastic.NewClient(elastic.SetURL([]string{"http://localhost:9200/"})) //instantiate a new es client object instance
	if err != nil {
		fmt.Printf("Failed to initiate elasticsearch client", err)
	}

	exists, err := client.IndexExists(indexName).Do(ctx) //check whether given index exists
	if err != nil {
		fmt.Printf("Index name already exists", err)
	}
	if !exists {
		_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx) //create if non-existent
		if err != nil {
			fmt.Printf("Failed to create index", err)
		}
	}

	/////writes in data/////

	for id, content := range newsContent[indexName] {
		doc, _ := client.Index().
		Index(indexName) //writes in index name
		Id(strconv.Itoa(id)). //writes in id
		BodyJson(content). //writes in data content
		Refresh("wait_for").
		Do(ctx) //executes

		/////check for successful store/////
		result, _ := client.Get().
			Index(indexName).
			Id(strconv.Itoa(id)).
			Do(ctx)

		if result.Found == false{
			fmt.Printf("Failed to store and retrieve data", err)
			return false
		}	
	}
	
	return true
	
}


func main() {
	//set up news 
	go GetZongHeNews()
	go GetTouTiaoNews()

	//store data in elasticsearch
	go StoreInES()

	//deliver msg
	go DeliverMsgToKafka("ZHNews")
	go DeliverMsgToKafka("TTNews")

	//fetch msg
	go ConsumeMsgFromKafka([]string{"ZHNews", "TTNews"})

	// fmt.Printf("%+v\n", GetZongHeNews())
	// fmt.Printf("\n")
	// fmt.Printf("%+v\n", GetTouTiaoNews())
}
