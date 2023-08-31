package adapter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mediocregopher/radix/v4"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

// ScanOptions options for scanning keyspace
type ScanOptions struct {
	Pattern    string
	ScanCount  int
	Throttle   int
	SamplePerc int
}

type scanResult struct {
	cur  string
	keys []string
}

func (s *scanResult) UnmarshalRESP(br resp.BufferedReader, o *resp.Opts) error {
	var ah resp3.ArrayHeader
	if err := ah.UnmarshalRESP(br, o); err != nil {
		return err
	} else if ah.NumElems != 2 {
		return errors.New("not enough parts returned")
	}

	var c resp3.BlobString
	if err := c.UnmarshalRESP(br, o); err != nil {
		return err
	}

	s.cur = c.S
	s.keys = s.keys[:0]

	return resp3.Unmarshal(br, &s.keys, o)
}

// NewRedisService creates RedisService
func NewRedisService(client radix.Client) RedisService {
	return RedisService{
		client: client,
	}
}

// RedisService implementation for iteration over redis
type RedisService struct {
	client radix.Client
}

type BulkKeyInfo struct {
	Keys  []string
	Sizes []int64
}

// ScanKeys scans keys asynchroniously and sends them to the returned channel
func (s RedisService) ScanKeys(ctx context.Context, options ScanOptions) <-chan BulkKeyInfo {
	resultChan := make(chan BulkKeyInfo, options.ScanCount*2)

	// if options.Pattern != "*" && options.Pattern != "" {
	// 	scanOpts.Pattern = options.Pattern
	// }

	go func() {
		defer close(resultChan)
		scanRes := scanResult{cur: "0"}

		for {
			err := s.client.Do(ctx, radix.Cmd(&scanRes, "SCAN", scanRes.cur, "COUNT", strconv.Itoa(options.ScanCount)))
			if err != nil {
				fmt.Printf("Failed: %s\n", err.Error())
				return
			}

			sampledKeyLength := len(scanRes.keys) * 100 / options.SamplePerc
			keyIndex := 0
			ret := BulkKeyInfo{
				Keys:  make([]string, sampledKeyLength),
				Sizes: make([]int64, sampledKeyLength),
			}
			// keys := make([]string, 0, len(scanRes.keys)/10+1)
			// sizes := make([]string, 0, len(scanRes.keys)/10+1)
			p := radix.NewPipeline()
			for index, key := range scanRes.keys {
				if index%100 < options.SamplePerc {
					ret.Keys[keyIndex] = key
					p.Append(radix.Cmd(&ret.Sizes[keyIndex], "MEMORY", "USAGE", key))
					keyIndex++
				}
			}

			ret.Keys = ret.Keys[:keyIndex]
			ret.Sizes = ret.Sizes[:keyIndex]

			err = s.client.Do(ctx, p)
			if err != nil {
				fmt.Printf("Failed: %s\n", err.Error())
			}

			resultChan <- ret

			if scanRes.cur == "0" {
				return
			}

			if options.Throttle > 0 {
				time.Sleep(time.Nanosecond * time.Duration(options.Throttle))
			}
		}
	}()

	return resultChan
}

// GetKeysCount returns number of keys in the current database
func (s RedisService) GetKeysCount(ctx context.Context) (int64, error) {
	var keysCount int64
	err := s.client.Do(context.Background(), radix.Cmd(&keysCount, "DBSIZE"))
	if err != nil {
		return 0, err
	}

	return keysCount, nil
}

// GetMemoryUsage returns memory usage of given key
func (s RedisService) GetMemoryUsage(ctx context.Context, key string) (int64, error) {
	var res int64
	err := s.client.Do(context.Background(), radix.Cmd(&res, "MEMORY", "USAGE", key))
	if err != nil {
		return 0, err
	}

	return res, nil
}
