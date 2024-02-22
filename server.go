package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"runtime"
	"time"
)

type TemperatureAveragesRequest struct {
	Endpoint string `json:"endpoint"`
	Count    int    `json:"count"`
	RacerID  string `json:"racerId"`
}

type TemperatureAveragesResponse struct {
	RacerID string  `json:"racerId"`
	RaceID  string  `json:"raceId"`
	Time    float32 `json:"time"`
}

func main() {
	runtime.GOMAXPROCS(2)
	bucket := "speedway-internal"
	path := "scorekeeper/1brc"
	r := gin.Default()

	racerClient := http.Client{
		Timeout: 2 * time.Hour, // max 2 hours
	}

	r.POST("/timed/temperature-averages", func(c *gin.Context) {
		storage, err := NewCloudStorage(c.Request.Context(), bucket, path)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		var req TemperatureAveragesRequest
		decoder := json.NewDecoder(c.Request.Body)
		err = decoder.Decode(&req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		storedStationReader, err := storage.GetStationData(c.Request.Context(), req.Count)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer storedStationReader.Close()
		pipeline, err := NewMeasurementsJsonPipeline(storedStationReader)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer pipeline.Close()

		// conditionally gzip
		averagesRequest, errch := CreateRacerRequest(req, pipeline)
		// start timer
		start := time.Now()
		// stream to endpoint
		racerResponse, err := racerClient.Do(averagesRequest)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err = <-errch; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if racerResponse.StatusCode != 200 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("racer status %d", racerResponse.StatusCode)})
		}
		buf := bytes.Buffer{}
		_, err = buf.ReadFrom(racerResponse.Body)
		elapsed := time.Since(start)
		defer racerResponse.Body.Close()

		// receive full response
		// end timer
		// check response
		// - racerId
		// - averages
		c.JSON(http.StatusOK, gin.H{"temp": fmt.Sprintf("elapsed: %.5f", elapsed.Seconds())})
	})

	r.Run()
}

func CreateRacerRequest(req TemperatureAveragesRequest, pipeline *MeasurementsJsonPipeline) (*http.Request, <-chan error) {
	errch := make(chan error, 1)

	racerUrl := fmt.Sprintf("%s/1brc", req.Endpoint)
	if req.Count < 10000000 {
		defer close(errch)
		httpRequest, err := http.NewRequest("POST", racerUrl, pipeline)
		if err != nil {
			errch <- err
		}
		return httpRequest, errch
	} else {
		compressedBodyReader := CompressRequestBody(pipeline, errch)
		averagesRequest, err := http.NewRequest("POST", racerUrl, compressedBodyReader)
		if err != nil {
			errch <- err
			close(errch)
			return nil, errch
		}
		averagesRequest.Header.Add("Content-Encoding", "gzip")
		return averagesRequest, errch
	}
}

func CompressRequestBody(pipeline *MeasurementsJsonPipeline, errch chan<- error) io.ReadCloser {
	bodyReader, bodyWriter := io.Pipe()
	compressor := gzip.NewWriter(bodyWriter)
	go func() {
		defer close(errch)
		sentErr := false
		sendErr := func(err error) {
			if !sentErr {
				errch <- err
				sentErr = true
			}
		}

		if _, err := io.Copy(compressor, pipeline); err != nil && err != io.ErrClosedPipe {
			sendErr(err)
		}
		if err := compressor.Close(); err != nil && err != io.ErrClosedPipe {
			sendErr(err)
		}
		if err := bodyWriter.Close(); err != nil && err != io.ErrClosedPipe {
			sendErr(err)
		}
	}()
	return bodyReader
}
