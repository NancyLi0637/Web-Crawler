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

var newsSummary map[string][]StdNews
var newsContent map[string][][]string

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
		newsSummary["WYNews"] = append(newsSummary["WYNews"], item)
	}

}

func checkError(msg string, err error) {
	if err != nil {
		fmt.Println(msg, err)
		os.Exit(1)
	}
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

		checkError("Failed to fetch news", err)

		body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
		resp.Body.Close()                      //断开连接
		bodyStr := string(body)
		bodyCut := bodyStr[9 : len(bodyStr)-1]
		bodyJSON := []byte(bodyCut)

		checkError("Failed to read news", err)

		/////格式转换/////
		var wangyiNews WangYiNewsRaw                        //创造object
		err = json.Unmarshal([]byte(bodyJSON), &wangyiNews) //json转换到go
		checkError("Failed to unmarshal json", err)
		WYToStd(wangyiNews) //转换到标准格式
	}

	/////正文爬取/////
	GetNewsContent(newsSummary["WYNews"], 2)

}

func WangYiTest() {
	url := "https://3g.163.com/touch/reconstruct/article/list/BAI6RHDKwangning/0-10.html"

	resp, err := http.Get(url) //接口接入、get返回示例
	checkError("Failed to fetch news", err)

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
	resp.Body.Close()                      //断开连接
	bodyStr := string(body)
	bodyCut := bodyStr[9 : len(bodyStr)-1]
	bodyJSON := []byte(bodyCut)

	checkError("Failed to read news", err)

	/////格式转换/////
	var wangyiNews WangYiNewsRaw                        //创造object
	err = json.Unmarshal([]byte(bodyJSON), &wangyiNews) //json转换到go
	checkError("Failed to unmarshal json", err)
	WYToStd(wangyiNews) //转换到标准格式

	GetNewsContent(newsSummary["WYNews"], 2)
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
			chineseContent := chineseRegExp.FindAllString(string(body), -1) //find all the chinese content

			if id == 1 {
				newsContent["ZHNews"] = append(newsContent["ZHNews"], chineseContent)
				WriteToFile(each, chineseContent) //write news info and content into file
			} else if id == 2 {
				newsContent["WYNews"] = append(newsContent["WYNews"], chineseContent)
				WriteToFile(each, chineseContent) //write news info and content into file
			} else {
				newsContent["WYNews"] = append(newsContent["WYNews"], []string{each.Body})
				WriteToFile(each, []string{each.Body}) //write news info and content into file
			}
		}
	}
}

//WriteToFile stores data from API into "newsData.txt"
func WriteToFile(news StdNews, content []string) {
	f, _ := os.OpenFile("newsData.txt", os.O_WRONLY|os.O_APPEND, 0600)
	data, err := json.Marshal(news)

	checkError("Failed to marshal news", err)

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
func DeliverMsgToKafka(topic string) []string {
	//access data stored in [newsContent]
	//data := newsContent[topic]
	msgs := []string{}
	data := [][]string{[]string{topic}}

	//initiate a new producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:9092"})

	//check for successful creation of producer before proceeding
	checkError("Failed to create producer: %s\n", err)

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
					msg := "Delivery message to topic " + topic + string(ev.TopicPartition.Partition) + " at offset " + string(ev.TopicPartition.Offset) + "\n"
					msgs = append(msgs, msg)
				}
			}
		}
	}()

	//wait for message deliveries before shutting down
	producer.Flush(15 * 1000)

	return msgs

}

func connect(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	newsSummary = make(map[string][]StdNews)  //initiate [newsSummary]
	newsContent = make(map[string][][]string) //initiate [newsContent]

	//crawl news
	//GetWangYiNews()
	WangYiTest()

	//produce msg to kafka
	msgs := DeliverMsgToKafka("WYNews")
	for _, msg := range msgs {
		fmt.Fprintf(w, msg)
	}

}

func main() {
	http.HandleFunc("/crawlnews", connect)
	err := http.ListenAndServe(":9090", nil)
	checkError("Failed to connect to server", err)

}
