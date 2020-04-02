package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
)

const minimumSize = 500
const bufferSize = 6000000

var linesSeparator = []byte{10, 13}

type request struct {
	Text       string
	Content_id int
	Client_id  int
	Timestamp  int64
}

type client struct {
	id         int
	postBodies [][]byte
	length     int
	dataToSend []byte
	currentDay time.Time
	path       string
}

var clientsData [10]*client

func getDayBeginning(tm time.Time) time.Time {
	year, month, day := tm.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func isDifferentDay(c *client, tm time.Time) bool {
	diff := tm.Sub(c.currentDay)
	return diff.Hours() >= 24
}

func getTimeFromUnix(timestampMs int64) time.Time {
	seconds := timestampMs / 1000
	nanoseconds := (timestampMs % 1000) * 1000
	return time.Unix(seconds, nanoseconds)
}

func (c *client) prepareForFlush() {
	c.dataToSend = c.dataToSend[:0]
	for i, b := range c.postBodies {
		c.dataToSend = append(c.dataToSend, b...)
		c.dataToSend = append(c.dataToSend, linesSeparator...)
		c.postBodies[i] = nil
	}

	c.postBodies = c.postBodies[:0]
	c.length = 0

	date := c.currentDay.Format("2006-01-02")
	c.path = "/chat/" + date + "/content_logs_" + date + "_" + strconv.Itoa(c.id)
}

func (c *client) uploadPart() {

}

func (c *client) completeUploading() {

}

func (c *client) flush() {
	c.prepareForFlush()
	go c.uploadPart()
}

func (c *client) flushFinally() {
	c.prepareForFlush()

	go func() {
		c.uploadPart()
		c.completeUploading()
	}()
}

func fastHTTPHandler(ctx *fasthttp.RequestCtx) {
	var postData request
	postBody := ctx.PostBody()

	if err := json.Unmarshal(postBody, &postData); err != nil {
		panic(err)
	}
	ctx.SetStatusCode(201)
	c := clientsData[postData.Client_id-1]

	tm := getTimeFromUnix(postData.Timestamp)

	if c.currentDay.IsZero() {
		c.currentDay = getDayBeginning(tm)
	} else if isDifferentDay(c, tm) {
		c.flushFinally()
		c.currentDay = getDayBeginning(tm)
		return
	}

	c.postBodies = append(c.postBodies, postBody)
	c.length += len(postBody) + len(linesSeparator)
	if c.length > minimumSize {
		c.flush()
	}
}

func main() {
	for i := range clientsData {
		clientsData[i] = &client{
			id:         i + 1,
			dataToSend: make([]byte, 0, bufferSize),
		}
	}

	sigs := make(chan os.Signal)
	programIsFinished := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs

		for _, c := range clientsData {
			if c.length > 0 {
				c.flushFinally()
			}
		}

		programIsFinished <- true
	}()

	go fasthttp.ListenAndServe(":8080", fastHTTPHandler)

	<-programIsFinished
}
