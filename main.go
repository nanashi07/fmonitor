package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"github.com/radovskyb/watcher"
	"log"
	"os"
	"path"
	"time"
)

// 程式設定
type appConfig struct {
	// 主機位址
	Host string
	// 連接埠
	Port int
	// 資料庫名稱
	Database string
	// 登入帳號
	User string
	// 登入密碼
	Password string
	// 主機名稱
	Source string
	// 監控檔案
	MonitorFiles []string
}

type CrossData struct {
	Id        string
	Source    string
	ActionAt  int64
	CreatedAt time.Time
}

// 時間顯示格式
const timeFormat = "2006-01-02 15:04:05.000000000"

var (
	Config = new(appConfig)
)

func queryLastOne(connection *string) *CrossData {
	var db *sql.DB
	if dbl, err := sql.Open("mysql", *connection); err != nil {
		log.Fatalln(err)
	} else {
		db = dbl

		// 關閉資料庫
		defer func() {
			db.Close()
		}()
	}

	if result, err := db.Query("select Id,Source,ActionAt,CreatedAt from CrossData order by CreatedAt desc limit 0,1"); err != nil {
		log.Fatalln(err)
	} else {
		var id string
		var source string
		var actionAt int64
		var createdAt time.Time

		// 讀取資料
		if result.Next() {
			// 關閉
			defer func() {
				result.Close()
			}()

			result.Scan(&id, &source, &actionAt, &createdAt)

			data := new(CrossData)
			data.Id = id
			data.Source = source
			data.ActionAt = actionAt
			data.CreatedAt = createdAt

			return data
		}
	}

	return nil
}

func main() {
	// 取得設定檔位置
	configPath := os.Args[1]

	// 處理設定檔路徑
	if !path.IsAbs(configPath) {
		// 取得當前執行目錄
		base, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		configPath = path.Join(base, configPath)
	}

	// 載入設定
	if _, err := toml.DecodeFile(configPath, &Config); err != nil {
		panic(fmt.Sprintf("%T 讀取設定失敗 %v", err, err))
	} else {
		json.Marshal(Config)
	}

	connection := fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=utf8&parseTime=true",
		Config.User,
		Config.Password,
		Config.Host,
		Config.Port,
		Config.Database)

	// 建立監聽器
	w := watcher.New()

	// 處理監聽事件
	go func() {
		for {
			select {
			case <-w.Event:
				// 同步時間
				eventTime := time.Now().UTC().Format(timeFormat)

				// 取得最後一筆資料
				data := queryLastOne(&connection)

				// 輸出記錄
				fmt.Printf("%v,Receive data,%v,%v,%v,%v,%v\n",
					eventTime,
					data.Id,
					data.Source,
					Config.Source, // target
					data.ActionAt,
					data.CreatedAt.Format(timeFormat))
			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	// 取得參數（要監聽的檔案）
	for index := range Config.MonitorFiles {
		fileName := Config.MonitorFiles[index]

		// 監聽指定檔案
		if err := w.Add(fileName); err != nil {
			log.Fatalln(err)
		}
	}

	// 啟動監聽，每毫秒處理一次
	if err := w.Start(time.Millisecond); err != nil {
		log.Fatalln(err)
	}

}
