package main

import (
	"encoding/json"
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
	return time.Now()
}

func isDifferentDay(c *client, tm time.Time) bool {
	return false
}

func getTimeFromUnix(timestampMs int64) time.Time {
	return time.Now()
}

func (c *client) prepareForFlush() {
	// TODO: combine data from POST bodies and prepare it for sending
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

	go fasthttp.ListenAndServe(":8080", fastHTTPHandler)
}
