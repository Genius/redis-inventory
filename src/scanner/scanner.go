package scanner

import (
	"context"

	"github.com/obukhov/redis-inventory/src/adapter"
	"github.com/obukhov/redis-inventory/src/trie"
	"github.com/rs/zerolog"
)

// RedisServiceInterface abstraction to access redis
type RedisServiceInterface interface {
	ScanKeys(ctx context.Context, options adapter.ScanOptions) <-chan adapter.BulkKeyInfo
	GetKeysCount(ctx context.Context) (int64, error)
	GetMemoryUsage(ctx context.Context, key string) (int64, error)
}

// RedisScanner scans redis keys and puts them in a trie
type RedisScanner struct {
	redisService RedisServiceInterface
	scanProgress adapter.ProgressWriter
	logger       zerolog.Logger
}

// NewScanner creates RedisScanner
func NewScanner(redisService RedisServiceInterface, scanProgress adapter.ProgressWriter, logger zerolog.Logger) *RedisScanner {
	return &RedisScanner{
		redisService: redisService,
		scanProgress: scanProgress,
		logger:       logger,
	}
}

// Scan initiates scanning process
func (s *RedisScanner) Scan(options adapter.ScanOptions, result *trie.Trie) {
	var totalCount int64
	if options.Pattern == "*" || options.Pattern == "" {
		totalCount = s.getKeysCount()
	}

	s.scanProgress.Start((totalCount * int64(options.SamplePerc)) / 100)
	for keyResult := range s.redisService.ScanKeys(context.Background(), options) {
		for index, key := range keyResult.Keys {
			s.scanProgress.Increment()

			result.Add(
				key,
				trie.ParamValue{Param: trie.BytesSize, Value: keyResult.Sizes[index] * int64(100/options.SamplePerc)},
				trie.ParamValue{Param: trie.KeysCount, Value: int64(100 / options.SamplePerc)},
			)

			if index%10 == 0 {
				s.logger.Debug().Msgf("Dump %s value: %d", key, keyResult.Sizes[index])
			}
		}
	}
	s.scanProgress.Stop()
}

func (s *RedisScanner) getKeysCount() int64 {
	res, err := s.redisService.GetKeysCount(context.Background())
	if err != nil {
		s.logger.Error().Err(err).Msgf("Error getting number of keys")
		return 0
	}

	return res
}
