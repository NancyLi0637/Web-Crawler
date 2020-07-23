package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var zhNewsSummary []StdNews
var ttNewsSummary []StdNews
var wyNewsSummary []StdNews
var zhNewsContent [][]string
var ttNewsContent [][]string
var wyNewsContent [][]string
var newsMap map[string][][]string

//create a mapping for es data storing
// const mapping = `
// 	{
// 		"mappings": {
// 			"properties": {
// 				"ctime": {
// 					"type": "text"
// 				},
// 				"title": {
// 					"type": "text"
// 				},
// 				"description": {
// 					"type": "text"
// 				}
// 				"url": {
// 					"type": "text"
// 				}
// 				"source": {
// 					"type": "text"
// 				}
// 			}
// 		}
// 	}`

/////数据归一化/////

//ZongHeNewsRaw contains raw material from API
type ZongHeNewsRaw struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	Newslist []struct {
		Ctime       string `json:"ctime"`
		Title       string `json:"title"`
		Description string `json:"description"`
		PicURL      string `json:"picUrl"`
		URL         string `json:"url"`
	} `json:"newslist"`
}

//TouTiaoNewsRaw contains a raw material form API
type TouTiaoNewsRaw struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	Newslist []struct {
		Ctime       string `json:"ctime"`
		Title       string `json:"title"`
		Description string `json:"description"`
		PicURL      string `json:"picUrl"`
		URL         string `json:"url"`
		Source      string `json:"source"`
	} `json:"newslist"`
}

//WangYiNewsRaw contains a raw material form API
type WangYiNewsRaw map[string][]struct {
	LiveInfo     interface{} `json:"liveInfo"`
	Docid        string      `json:"docid"`
	Source       string      `json:"source"`
	Title        string      `json:"title"`
	Priority     int         `json:"priority"`
	HasImg       int         `json:"hasImg"`
	URL          string      `json:"url"`
	CommentCount int         `json:"commentCount"`
	Imgsrc3Gtype string      `json:"imgsrc3gtype"`
	Stitle       string      `json:"stitle"`
	Digest       string      `json:"digest"`
	Imgsrc       string      `json:"imgsrc"`
	Ptime        string      `json:"ptime"`
	Imgextra     []struct {
		Imgsrc string `json:"imgsrc"`
	} `json:"imgextra,omitempty"`
}

//StdNews object is in standard format
type StdNews struct {
	Timestamp string   `json:"timestamp`
	Source    string   `json:"source"`
	Title     string   `json:"title"`
	Body      string   `json:"body"`
	PicURL    string   `json:"picurl`
	URL       string   `json:"url"`
	Types     []string `json:"types"`
	Keywords  []string `json:keywords"`
}

//ZHToStd changes raw news to standard format
func ZHToStd(news ZongHeNewsRaw) {

	for _, each := range news.Newslist {
		var item StdNews
		item.Timestamp = time.Now().Format("2006-01-02 15:04:05")
		item.Source = "ZongHeNews"
		item.Title = each.Title
		item.Body = each.Description
		item.URL = each.URL
		item.PicURL = each.PicURL
		zhNewsSummary = append(zhNewsSummary, item)
	}

}

//TTToStd changes raw news to standard format
func TTToStd(news TouTiaoNewsRaw) {

	for _, each := range news.Newslist {
		var item StdNews
		item.Timestamp = time.Now().Format("2006-01-02 15:04:05")
		item.Source = each.Source
		item.Title = each.Title
		item.Body = each.Description
		item.URL = each.URL
		item.PicURL = each.PicURL
		ttNewsSummary = append(ttNewsSummary, item)
	}
}

//WYToStd changes raw news to standard format
func WYToStd(news WangYiNewsRaw) {

	var key string
	for k := range news {
		key = k
	}

	for _, each := range news[key] {
		var item StdNews
		item.Timestamp = time.Now().Format("2006-01-02 15:04:05")
		item.Source = each.Source
		item.Title = each.Title
		item.Body = each.Digest
		item.URL = each.URL
		item.PicURL = each.Imgsrc
		wyNewsSummary = append(wyNewsSummary, item)
	}

}

/////数据接入/////

//GetZongHeNews set-up in [zhNewsSummary] and [zhNewsContent]
func GetZongHeNews() {
	url := "http://api.tianapi.com/generalnews/index?key=d9455e812137a7cd7c9ab10229c34ec6" //ZHNews url
	resp, err := http.Get(url)                                                             //接口接入、get返回示例

	if err != nil {
		fmt.Printf("Failed to fetch news", err)
	}

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
	resp.Body.Close()                      //断开连接

	if err != nil {
		fmt.Printf("Failed to read news", err)
	}

	/////格式转换/////
	var zongheNews ZongHeNewsRaw                    //创造object
	err = json.Unmarshal([]byte(body), &zongheNews) //json转换到go
	if err != nil {
		fmt.Println("Failed to unmarshal json", err)
	}
	ZHToStd(zongheNews) //转换到标准格式

	/////正文爬取/////
	GetNewsContent(zhNewsSummary, 1)
	newsMap = make(map[string][][]string)
	newsMap["ZHNews"] = zhNewsContent

}

//GetTouTiaoNews set-up in [ttNewsSummary] and [ttNewsContent]
func GetTouTiaoNews() {
	url := "http://api.tianapi.com/topnews/index?key=d9455e812137a7cd7c9ab10229c34ec6"
	resp, err := http.Get(url) //接口接入、get返回示例

	if err != nil {
		fmt.Printf("Failed to fetch news", err)
	}

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式
	resp.Body.Close()                      //断开连接

	if err != nil {
		fmt.Printf("Failed to read news", err)
	}

	/////格式转换/////
	var toutiaoNews TouTiaoNewsRaw //创造object
	err = json.Unmarshal([]byte(body), &toutiaoNews)
	if err != nil {
		fmt.Println("Failed to unmarshal json", err)
	}
	TTToStd(toutiaoNews) //转换到标准格式

	/////正文爬取/////
	GetNewsContent(ttNewsSummary, 3)
	newsMap = make(map[string][][]string)
	newsMap["TTNews"] = ttNewsContent

}

//GetWangYiNews set-up in [wyNewsSummary] and [wyNewsContent]
func GetWangYiNews() {

	//网易新闻API接口{“游戏”，“教育”，“新闻”，“娱乐”，“体育”，“财经”，“军事”，“科技”，“手机”，“数码”，“时尚”，“健康”，“旅游”}
	urls := []string{"https://3g.163.com/touch/reconstruct/article/list/BAI6RHDKwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA8FF5PRwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BBM54PGAwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA10TA81wangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA8E6OEOwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA8EE5GMwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BAI67OGGwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA8D4A3Rwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BAI6I0O5wangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BAI6JOD9wangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BA8F6ICNwangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BDC4QSV3wangning/0-10.html",
		"https://3g.163.com/touch/reconstruct/article/list/BEO4GINLwangning/0-10.html"}

	for _, url := range urls {
		resp, err := http.Get(url) //接口接入、get返回示例

		if err != nil {
			fmt.Printf("Failed to fetch news", err)
		}

		body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
		resp.Body.Close()                      //断开连接
		bodyStr := string(body)
		bodyCut := bodyStr[9 : len(bodyStr)-1]
		bodyJSON := []byte(bodyCut)

		if err != nil {
			fmt.Printf("Failed to read news", err)
		}

		/////格式转换/////
		var wangyiNews WangYiNewsRaw                        //创造object
		err = json.Unmarshal([]byte(bodyJSON), &wangyiNews) //json转换到go
		if err != nil {
			fmt.Println("Failed to unmarshal json", err)
		}
		WYToStd(wangyiNews) //转换到标准格式

		/////正文爬取/////
		GetNewsContent(wyNewsSummary, 2)
		newsMap["WYNews"] = wyNewsContent
	}

}

//GetNewsContent extracts all the chinese content in string form
func GetNewsContent(news []StdNews, id int) {
	chineseRegExp := regexp.MustCompile("[\\p{Han}]+") //正则匹配中文格式

	for _, each := range news {
		url := each.URL
		resp, err := http.Get(url) //access news article through url
		if err == nil {
			body, _ := ioutil.ReadAll(resp.Body) //get html body
			resp.Body.Close()

			respStr := string(body)                                    //change html into string
			chineseContent := chineseRegExp.FindAllString(respStr, -1) //find all the chinese content

			if id == 1 {
				zhNewsContent = append(zhNewsContent, chineseContent)
				WriteToFile(each, chineseContent) //write news info and content into file
			} else if id == 2 {
				wyNewsContent = append(wyNewsContent, chineseContent)
				WriteToFile(each, chineseContent) //write news info and content into file
			} else {
				ttNewsContent = append(ttNewsContent, []string{each.Body})
				WriteToFile(each, []string{each.Body}) //write news info and content into file
			}
		}
	}
}

//WriteToFile stores data from API into "backUp.txt"
func WriteToFile(news StdNews, content []string) {
	f, _ := os.OpenFile("newsData.txt", os.O_WRONLY|os.O_APPEND, 0600)
	data, err := json.Marshal(news)

	if err != nil {
		fmt.Println("Failed to marshal news", err)
	}

	f.Write(data) //write marshaled news info in json format
	f.WriteString("\n")
	for _, s := range content {
		f.WriteString(s)
	}
	f.WriteString("\n\n")
	f.Close()
}

///kafka数据发送与接收/////

//DeliverMsgToKafka sends message to brokers with distinct topics
func DeliverMsgToKafka(topic string) {
	//access data stored in [newsContent]
	data := newsMap[topic]

	//initiate a new producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:9092"})

	//check for successful creation of producer before proceeding
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err) //error message
		os.Exit(1)
	}

	/////WRITES MESSAGE/////

	//produce message
	for _, news := range data {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(news[0]),
		}, nil)
	}

	//prints out message
	fmt.Println("Produced message, waiting for delivery response.")

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
					fmt.Printf("Delivery message to topic %s [%d] at offset %v\n",
						topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	//wait for message deliveries before shutting down
	producer.Flush(15 * 1000)

}

//ConsumeMsgFromKafka consumes messages in the kafka brokers with distinct topics
func ConsumeMsgFromKafka(topic string) {
	//initiate a new consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "myNews",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
	}

	//controls topic fetched
	consumer.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)

	for {
		//consumer poll message
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		}
	}

	consumer.Close()
}

// func StoreInES(string indexName)(bool){

// 	/////initiate a new es client/////
// 	ctx := context.Background() //create a context object for API calls
// 	client, err := elastic.NewClient(elastic.SetURL([]string{"http://localhost:9200/"})) //instantiate a new es client object instance
// 	if err != nil {
// 		fmt.Printf("Failed to initiate elasticsearch client", err)
// 	}

// 	exists, err := client.IndexExists(indexName).Do(ctx) //check whether given index exists
// 	if err != nil {
// 		fmt.Printf("Index name already exists", err)
// 	}
// 	if !exists {
// 		_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx) //create if non-existent
// 		if err != nil {
// 			fmt.Printf("Failed to create index", err)
// 		}
// 	}

// 	/////writes in data/////

// 	for id, content := range newsContent[indexName] {
// 		doc, _ := client.Index().
// 		Index(indexName) //writes in index name
// 		Id(strconv.Itoa(id)). //writes in id
// 		BodyJson(content). //writes in data content
// 		Refresh("wait_for").
// 		Do(ctx) //executes

// 		/////check for successful store/////
// 		result, _ := client.Get().
// 			Index(indexName).
// 			Id(strconv.Itoa(id)).
// 			Do(ctx)

// 		if result.Found == false{
// 			fmt.Printf("Failed to store and retrieve data", err)
// 			return false
// 		}
// 	}

// 	return true

// }

func main() {

	//set up news
	//GetZongHeNews()
	GetTouTiaoNews()
	//GetWangYiNews()

	//store data in elasticsearch
	//go StoreInES()

	//deliver msg
	//DeliverMsgToKafka("ZHNews")
	DeliverMsgToKafka("TTNews")
	//DeliverMsgToKafka("WYNews")

	//fetch msg
	//ConsumeMsgFromKafka("ZHNews")
	ConsumeMsgFromKafka("TTNews")
	//ConsumeMsgFromKafka("WYNews")

	// fmt.Printf("%+v\n", GetZongHeNews())
	// fmt.Printf("\n")
	// fmt.Printf("%+v\n", GetTouTiaoNews())
}
