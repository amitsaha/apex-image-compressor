package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/apex/go-apex"
	log "github.com/sirupsen/logrus"
)

type message struct {
	Bucket   string `json:"bucket"`
	ObjectID string `json:"s3obj_id"`
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stderr)

}

func preProcess(imageData []byte) []byte {
	log.WithFields(log.Fields{
		"Raw bytes preProcess": len(imageData),
	}).Info()

	processedData := make([]byte, len(imageData))

	// Work with processed data

	log.WithFields(log.Fields{
		"Preprocessed bytes": len(processedData),
	}).Info()

	return processedData
}

func postProcess() {

}

func getImageBytes(bucket string, objectID string) ([]byte, error) {
	cfg := aws.NewConfig()
	awsSession := session.New(cfg)
	s3Svc := s3.New(awsSession)
	result, err := s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectID),
	})

	if err != nil {
		log.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, result.Body); err != nil {
		return nil, err
	}

	result.Body.Close()
	return buf.Bytes(), nil
}

func main() {
	apex.HandleFunc(func(event json.RawMessage, ctx *apex.Context) (interface{}, error) {
		var m message

		if err := json.Unmarshal(event, &m); err != nil {
			return nil, err
		}

		cmd := exec.Command("./bin/jpegtran")
		// To get access to the stderr
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		// Shared library
		cmd.Env = []string{"LD_LIBRARY_PATH=./bin"}

		// Feed in the raw bytes
		imageData, err := getImageBytes(m.Bucket, m.ObjectID)
		if err != nil {
			log.Fatal(err)
		}
		log.WithFields(log.Fields{
			"Raw bytes": len(imageData),
		}).Info()
		imageData = preProcess(imageData)

		stdin, err := cmd.StdinPipe()
		defer stdin.Close()

		if err != nil {
			log.Fatal(err)
		}

		go func() {
			defer stdin.Close()
			stdin.Write(imageData)
		}()

		compressedBytes, err := cmd.Output()
		if err != nil {
			log.Info(stderr.String())
			log.Fatal(err)
		}
		log.WithFields(log.Fields{
			"Compressed bytes": len(compressedBytes),
		}).Info()

		//TODO: Write back the compressed bytes

		return m, nil
	})
}
