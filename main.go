package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

//数据归一化
type ZongHeNews struct {
	Newslist []ZHNews `json:"newslist"` //获取“newslist”里的news(接口format)
}

type ZHNews struct {
	Timestamp   uint   `json:"ctime"` //转换news格式到规定格式
	Title       string `json:"title"`
	Description string `json:"description"`
	Url         string `json:"url"`
	// no source
}

type TouTiaoNews struct {
	NewsList []TTNews `json:"newslist"`
}

type TTNews struct {
	ZHNews        //继承ZHNews相同的格式
	Source string `json:"source"`
}

//数据接入
func getZongHeNews() interface{} {
	url := "http://api.tianapi.com/generalnews/index?key=d9455e812137a7cd7c9ab10229c34ec6"
	req, _ := http.NewRequest("GET", url, nil)
	res, _ := http.DefaultClient.Do(req) //接口接入、get返回示例

	defer res.Body.Close()              //断开连接延迟
	body, _ := ioutil.ReadAll(res.Body) //转成可读形式

	var zongheNews ZongHeNews                        //创造object
	err := json.Unmarshal([]byte(body), &zongheNews) //将json格式转换成标准格式
	if err != nil {
		fmt.Println(err)
	}

	return zongheNews //返回转换好的数据
}

func getTouTiaoNews() interface{} {
	url := "http://api.tianapi.com/topnews/index?key=d9455e812137a7cd7c9ab10229c34ec6"
	req, _ := http.NewRequest("GET", url, nil)
	res, _ := http.DefaultClient.Do(req) //接口接入、get返回示例

	defer res.Body.Close()              //断开连接延迟
	body, _ := ioutil.ReadAll(res.Body) //转成可读形式

	var toutiaoNews TouTiaoNews                       //创造object
	err := json.Unmarshal([]byte(body), &toutiaoNews) //将json格式转换成标准格式
	if err != nil {
		fmt.Println(err)
	}

	return toutiaoNews //返回转换好的数据
}

func main() {
	//deliverMsgToKafka("zongheNews", getZongHeNews())
	fmt.Printf("%+v\n", getZongHeNews())
	fmt.Printf("%+v\n", getTouTiaoNews)

}

// func deliverMsgToKafka() {}
