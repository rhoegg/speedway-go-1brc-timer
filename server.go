package main

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
	"net/http"
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
	bucket := "speedway-internal"
	path := "scorekeeper/1brc"
	r := gin.Default()

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
		// start timer
		// stream to endpoint
		racerClient := http.Client{
			Timeout: 2 * time.Hour, // max 2 hours
		}
		racerUrl := fmt.Sprintf("%s/1brc", req.Endpoint)
		averagesRequest, err := http.NewRequest("POST", racerUrl, pipeline)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		racerResponse, err := racerClient.Do(averagesRequest)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if racerResponse.StatusCode != 200 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("racer status %d", racerResponse.StatusCode)})
		}
		buf := bytes.Buffer{}
		_, err = buf.ReadFrom(racerResponse.Body)

		fmt.Printf("response: %s", buf.String())
		// receive full response
		// end timer
		// check response
		// - racerId
		// - averages
	})

	r.Run()
}
