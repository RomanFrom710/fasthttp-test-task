package main

import (
	"bytes"
	"compress/gzip"
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
const minimumSize = 5 * oneMb     // AWS restriction
const lengthCheckFrequency = 1000 // Each nth request for a particular client is checked if its gzip version greater than minimumSize
const bufferSize = 7 * oneMb      // So it can fit some requests between reaching minimumSize and length check

var linesSeparator = []byte{10, 13}

var wg sync.WaitGroup

type request struct {
	Text       string
	Content_id int
	Client_id  int
	Timestamp  int64
}

type client struct {
	id           int
	currentData  []byte
	dataToSend   bytes.Buffer
	requestCount int
	currentDay   time.Time
	path         string

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

func (c *client) createUploadingIfNotExist() {
	if c.uploading != nil {
		return
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:          aws.String("s3test-roman"),
		Key:             aws.String(c.path),
		ContentType:     aws.String("application/x-ndjson"),
		ContentEncoding: aws.String("gzip"),
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
		Body:          bytes.NewReader(c.dataToSend.Bytes()),
		Bucket:        c.uploading.Bucket,
		Key:           c.uploading.Key,
		PartNumber:    partNumber,
		UploadId:      c.uploading.UploadId,
		ContentLength: aws.Int64(int64(c.dataToSend.Len())),
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
	c.dataToSend.Reset()
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

func (c *client) prepareDataToSend() {
	w := gzip.NewWriter(&c.dataToSend)
	w.Write(c.currentData)
	w.Close()
}

func (c *client) preparePath() {
	date := c.currentDay.Format("2006-01-02")
	c.path = "/chat/" + date + "/content_logs_" + date + "_" + strconv.Itoa(c.id)
}

func (c *client) flush(isFinal bool) {
	wg.Add(1)
	c.preparePath()
	if c.dataToSend.Len() == 0 {
		c.prepareDataToSend()
	}
	c.requestCount = 0

	go func() {
		c.uploadMu.Lock()
		c.uploadPart()

		if isFinal {
			c.completeUploading()
		}
		c.uploadMu.Unlock()
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
		c.flush(true)
		c.currentDay = getDayBeginning(tm)
	}

	c.requestCount++
	c.currentData = append(c.currentData, postBody...)
	c.currentData = append(c.currentData, linesSeparator...)

	if c.requestCount%lengthCheckFrequency == 0 {
		c.prepareDataToSend()
		if c.dataToSend.Len() > minimumSize {
			c.flush(false)
		} else {
			c.dataToSend.Reset()
		}
	}
}

func main() {
	for i := range clientsData {
		clientsData[i] = &client{
			id:          i + 1,
			currentData: make([]byte, 0, bufferSize),
		}
		clientsData[i].dataToSend.Grow(bufferSize)
	}

	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-central-1")}))
	svc = s3.New(sess)

	sigs := make(chan os.Signal)
	programIsFinished := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs

		for _, c := range clientsData {
			c.mu.Lock()
			if len(c.currentData) > 0 {
				c.flush(true)
			}
			c.mu.Unlock()
		}

		programIsFinished <- true
	}()

	go fasthttp.ListenAndServe(":8080", fastHTTPHandler)

	<-programIsFinished
	wg.Wait()
}
