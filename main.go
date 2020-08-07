package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/olivere/elastic.v7"
)

var newsSummary map[string][]StdNews
var newsContent map[string][][]string

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
		newsSummary["ZHNews"] = append(newsSummary["ZHNews"], item)
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
		newsSummary["TTNews"] = append(newsSummary["TTNews"], item)
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
		newsSummary["WYNews"] = append(newsSummary["WYNews"], item)
	}

}

func checkError(msg string, err error) {
	if err != nil {
		fmt.Println(msg, err)
		os.Exit(1)
	}
}

/////数据接入/////

//GetZongHeNews set-up in [zhNewsSummary] and [zhNewsContent]
func GetZongHeNews() {
	url := "http://api.tianapi.com/generalnews/index?key=d9455e812137a7cd7c9ab10229c34ec6" //ZHNews url
	resp, err := http.Get(url)                                                             //接口接入、get返回示例

	checkError("Failed to fetch news", err)

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式(json)
	resp.Body.Close()                      //断开连接

	checkError("Failed to read news", err)

	/////格式转换/////
	var zongheNews ZongHeNewsRaw                    //创造object
	err = json.Unmarshal([]byte(body), &zongheNews) //json转换到go
	checkError("Failed to unmarshal json", err)
	ZHToStd(zongheNews) //转换到标准格式

	/////正文爬取/////
	GetNewsContent(newsSummary["ZHNews"], 1)

}

//GetTouTiaoNews set-up in [ttNewsSummary] and [ttNewsContent]
func GetTouTiaoNews() {
	url := "http://api.tianapi.com/topnews/index?key=d9455e812137a7cd7c9ab10229c34ec6"
	resp, err := http.Get(url) //接口接入、get返回示例

	checkError("Failed to fetch news", err)

	body, err := ioutil.ReadAll(resp.Body) //转成可读形式
	resp.Body.Close()                      //断开连接

	checkError("Failed to read news", err)

	/////格式转换/////
	var toutiaoNews TouTiaoNewsRaw //创造object
	err = json.Unmarshal([]byte(body), &toutiaoNews)
	checkError("Failed to unmarshal json", err)
	TTToStd(toutiaoNews) //转换到标准格式

	/////正文爬取/////
	GetNewsContent(newsSummary["TTNews"], 3)

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
	//GetNewsContent(newsSummary["WYNews"], 2)

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

//WriteToFile stores data from API into "backUp.txt"
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
func DeliverMsgToKafka(topic string) {
	//access data stored in [newsContent]
	//data := newsContent[topic]
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

	MatchQuery(ctx, client, "网易游戏频道") //search through client on MatchQuery
	fmt.Println("****")
}

//MatchQuery search through client on MatchQuery
func MatchQuery(ctx context.Context, client *elastic.Client, source string) {
	fmt.Printf("Search: %s ", source)
	// Term搜索
	matchQuery := elastic.NewMatchQuery("source", source)
	searchResult, err := client.Search().
		Index("wy_news2").
		Query(matchQuery).
		//Sort("source", true). // 按id升序排序
		From(0).Size(10). // 拿前10个结果
		Pretty(true).
		Do(ctx) // 执行
	if err != nil {
		panic(err)
	}
	total := searchResult.TotalHits()
	fmt.Printf("Found %d subjects\n", total)
	var std StdNews
	if total > 0 {
		for _, item := range searchResult.Each(reflect.TypeOf(std)) {
			if t, ok := item.(StdNews); ok {
				fmt.Printf("Found: Subject(source=%d, title=%s)\n", t.Source, t.Title)
			}
		}

	} else {
		fmt.Println("Not found!")
	}
}

//QuerySearch search through QueryDSL
func QuerySearch() {
	url := "http://localhost:9200/wy_news/_search"
	query := `{
		"query":{
		  "match" : {
			"source": "网易游戏"
		  }
		}
	  }`

	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(query)))
	checkError("Failed to fetch data", err)

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	checkError("Failed to read news", err)
	fmt.Println(string(body))
}

func sayhelloName(w http.ResponseWriter, r *http.Request) {
	WangYiTest()
}

func main() {

	newsSummary = make(map[string][]StdNews)  //initiate [newsSummary]
	newsContent = make(map[string][][]string) //initiate [newsContent]

	http.HandleFunc("/crawlnews", sayhelloName) //设置访问的路由
	err := http.ListenAndServe(":9090", nil)    //设置监听的端口
	checkError("Failed to request http service", err)

	//set up news
	//GetZongHeNews()
	//GetTouTiaoNews()
	//GetWangYiNews()
	//WangYiTest()

	//deliver msg
	//DeliverMsgToKafka("ZHNews")
	//DeliverMsgToKafka("TTNews")
	//DeliverMsgToKafka("WYNews")

	//fetch msg
	//ConsumeMsgFromKafka("ZHNews")
	//ConsumeMsgFromKafka("TTNews")
	//go ConsumeMsgFromKafka("WYNews")

	//store data in ES
	//StoreInES("wy_news2")
	//QuerySearch() //search through QueryDSL

	// fmt.Printf("%+v\n", GetZongHeNews())
	// fmt.Printf("\n")
	// fmt.Printf("%+v\n", GetTouTiaoNews())
}
