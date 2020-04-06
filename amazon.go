package main

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const region = "eu-central-1"
const bucket = "s3test-roman"

var svc *s3.S3

func initializeSvc() {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc = s3.New(sess)
}

func (c *client) createUploadingIfNotExist() {
	if c.uploading != nil {
		return
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:          aws.String(bucket),
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
