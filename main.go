package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/valyala/fasthttp"
)

const oneMb = 1024 * 1024
const minimumSize = 5 * oneMb
const bufferSize = 6 * oneMb

var linesSeparator = []byte{10, 13}

var wg sync.WaitGroup

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

	mu       sync.Mutex
	uploadMu sync.Mutex

	uploading      *s3.CreateMultipartUploadOutput
	completedParts []*s3.CompletedPart
}

var clientsData [10]*client

var svc *s3.S3

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

func (c *client) createUploadingIfNotExist() {
	if c.uploading != nil {
		return
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String("s3test-roman"),
		Key:         aws.String(c.path),
		ContentType: aws.String("application/x-ndjson"),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		panic(err)
	}
	c.uploading = resp
}

func (c *client) uploadPart() {
	fmt.Println("Uploading for " + c.path)
	c.createUploadingIfNotExist()

	partNumber := aws.Int64(int64(len(c.completedParts) + 1))
	partInput := s3.UploadPartInput{
		Body:          bytes.NewReader(c.dataToSend),
		Bucket:        c.uploading.Bucket,
		Key:           c.uploading.Key,
		PartNumber:    partNumber,
		UploadId:      c.uploading.UploadId,
		ContentLength: aws.Int64(int64(len(c.dataToSend))),
	}
	resp, err := svc.UploadPart(&partInput)
	if err != nil {
		panic(err)
	}
	completedPart := &s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: partNumber,
	}
	c.completedParts = append(c.completedParts, completedPart)
}

func (c *client) completeUploading() {
	fmt.Println("Finishing uploading for " + c.path)
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   c.uploading.Bucket,
		Key:      c.uploading.Key,
		UploadId: c.uploading.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: c.completedParts,
		},
	}
	_, err := svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		panic(err)
	}

	c.uploading = nil
	c.completedParts = nil
}

func (c *client) flush() {
	c.prepareForFlush()
	go c.uploadPart()
}

func (c *client) flushFinally() {
	c.prepareForFlush()

	wg.Add(1)
	go func() {
		c.uploadPart()
		c.completeUploading()
		wg.Done()
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentDay.IsZero() {
		c.currentDay = getDayBeginning(tm)
	} else if isDifferentDay(c, tm) {
		c.flushFinally()
		c.currentDay = getDayBeginning(tm)
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

	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-central-1")}))
	svc = s3.New(sess)

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
	wg.Wait()
}
