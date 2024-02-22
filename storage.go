package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"math/rand/v2"
)

type CloudStorage struct {
	S3Client *s3.Client
	Bucket   string
	Path     string
}

type StationData struct {
	Key    string
	Reader io.ReadCloser
}

func NewCloudStorage(ctx context.Context, bucket, path string) (*CloudStorage, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg)
	return &CloudStorage{
		S3Client: s3Client,
		Bucket:   bucket,
		Path:     path,
	}, nil
}

func (s *CloudStorage) GetStationData(ctx context.Context, count int) (StationData, error) {
	// prepare data
	// - validate count (1M or 1B only)
	prefix := ""
	switch count {
	case 1000000:
		prefix = "1Mstations"
	case 1000000000:
		prefix = "stations"
	default:
		return StationData{}, errors.New(fmt.Sprintf("unsupported count %d", count))
	}

	listResult, err := s.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(fmt.Sprintf("%s/%s", s.Path, prefix)),
	})
	objects := listResult.Contents
	key := ""
	switch prefix {
	case "1Mstations":
		index := rand.IntN(len(objects))
		key = *objects[index].Key
	case "stations":
		key = "scorekeeper/1brc/stations-2024-02-10 222443.csv.gz"
	default:
		return StationData{}, errors.New(fmt.Sprintf("unsupported station prefix %s", prefix))
	}

	getResult, err := s.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return StationData{}, err
	}

	return StationData{
		Key:    key,
		Reader: getResult.Body,
	}, err
}
