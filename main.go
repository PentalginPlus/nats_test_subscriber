package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/nats-io/nats.go"
)

type Text string

type UserMessage struct {
	Message Text `json:"text"`
	UserID  int64
}

var db *sql.DB

func main() {
	db, err := sql.Open("pgx", os.Getenv("AUTH_DB_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		panic(err)
	}
	defer ec.Close()

	requestChanRecv := make(chan *[]UserMessage)
	ec.BindRecvChan("nats_testing", requestChanRecv)

	var msgCount int
	var userID int64
	for {
		req := <-requestChanRecv

		for _, value := range *req {
			_, err = db.Exec("insert into nats_messages (message, userid) values ($1, $2)", string(value.Message), value.UserID)
			if err != nil {
				log.Println(err)
				return
			}
			msgCount++
		}
		userID = (*req)[0].UserID
		writeLog := fmt.Sprintf("%d messages from UserID %d have been written to DB\n", msgCount, userID)
		msgCount = 0
		row := db.QueryRow("select COUNT(message) from nats_messages where userid = $1", userID)
		var count int
		err := row.Scan(&count)
		if err != nil {
			log.Println(err)
		}
		msgAlert := writeLog + fmt.Sprintf("Total messages from UserID %d in DB: %d", userID, count)

		data := url.Values{
			"chat_id": {os.Getenv("NATS_BOT_CHATID")},
			"text":    {msgAlert},
		}

		query := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", os.Getenv("NATS_BOT_TOKEN"))
		_, err = http.PostForm(query, data)
		if err != nil {
			log.Fatal((err))
		}
	}
}
