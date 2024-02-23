package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"github.com/goccy/go-json"
	"io"
	"log"
	"strconv"
)

type Measurement struct {
	Station     string       `json:"station"`
	Temperature RoundedFloat `json:"temperature"`
}

type RoundedFloat float64

func (r RoundedFloat) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(r), 'f', 5, 64)), nil
}

type MeasurementsJsonPipeline struct {
	source       io.Reader
	decompressed io.ReadCloser
	csv          *csv.Reader
	rowsConsumed int64
	Epsilon      float64
	Buffer       *bytes.Buffer
}

func NewMeasurementsJsonPipeline(compressedCsvDataReader io.Reader, epsilon float64) (*MeasurementsJsonPipeline, error) {
	decompressed, err := gzip.NewReader(compressedCsvDataReader)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(decompressed)
	return &MeasurementsJsonPipeline{
		source:       compressedCsvDataReader,
		decompressed: decompressed,
		csv:          csvReader,
		Epsilon:      epsilon,
		Buffer:       bytes.NewBufferString("[\n"),
	}, nil
}

func (p *MeasurementsJsonPipeline) Close() error {
	return p.decompressed.Close()
}

func (p *MeasurementsJsonPipeline) Read(dest []byte) (int, error) {
	if p.Buffer.Len() == 0 {
		var m Measurement
		for i := 0; i < 500; i++ {
			record, err := p.csv.Read()
			if err != nil {
				if err == io.EOF {
					p.Buffer.WriteString("\n]")
					n, err := p.Buffer.Read(dest)
					if err != nil {
						return n, err
					}
					log.Printf("Completed downloading cached station data from %s", p.source)
					return n, io.EOF
				}
				return 0, err
			}
			if p.rowsConsumed == 0 && record[0] == "station" {
				record, err = p.csv.Read()
				if err != nil {
					return 0, err
				}
			}
			temp, err := strconv.ParseFloat(record[1], 64)
			if err != nil {
				return 0, err
			}
			m.Station = record[0]
			m.Temperature = RoundedFloat(temp + p.Epsilon)

			if p.rowsConsumed > 0 {
				p.Buffer.WriteString(",\n")
			}
			err = json.NewEncoder(p.Buffer).Encode(&m)
			if err != nil {
				return 0, err
			}
			p.rowsConsumed++
			if p.rowsConsumed%10000000 == 0 {
				log.Printf("[s3] Downloaded %d rows from %s", p.rowsConsumed, p.source)
			}
		}
	}
	copied, err := p.Buffer.Read(dest)
	if err == io.EOF {
		return copied, nil
	}
	return copied, err
}
