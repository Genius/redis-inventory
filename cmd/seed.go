package cmd

import (
	"context"
	"fmt"
	"github.com/mediocregopher/radix/v4"
	"github.com/obukhov/redis-inventory/src/logger"
	"github.com/obukhov/redis-inventory/src/seeder"
	"log"
	"math/rand"
	"time"

	"github.com/spf13/cobra"
)

var cycles int

var fillCmd = &cobra.Command{
	Use:   "seed [host:port]",
	Short: "Create random keys in redis instance",
	Args:  cobra.MinimumNArgs(1),
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Filling redis with random data")

		rand.Seed(time.Now().UTC().UnixNano())

		ctx := context.Background()
		redisClient, err := (radix.PoolConfig{}).New(ctx, "tcp", args[0])
		if err != nil {
			log.Fatal(err)
		}
		defer redisClient.Close()

		s := seeder.NewSeeder(redisClient, logger.NewConsoleLogger())

		hexGenerator := seeder.NewRandStringGenerator(4, 10, 'a', 'b', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
		s.Seed(
			seeder.NewGenericRecordGenerator(
				200,
				seeder.NewPatternStringGenerator(
					"%s:blogpost:%s:content",
					seeder.NewEnumStringGenerator("dev", "prod"),
					seeder.NewIntRangeStringGenerator(1, 5),
				),
				seeder.NewRandStringGenerator(100, 1000, '1', '0'),
				seeder.NewIntRangeGenerator(3600, 86400),
			),
			seeder.NewGenericRecordGenerator(
				100,
				seeder.NewPatternStringGenerator(
					"%s:blogpost:%s:comment:%s",
					seeder.NewEnumStringGenerator("dev", "prod"),
					seeder.NewIntRangeStringGenerator(1, 5),
					seeder.NewIntRangeStringGenerator(1, 1000),
				),
				seeder.NewRandStringGenerator(100, 1000, '1', '0'),
				seeder.NewIntRangeGenerator(3600, 86400),
			),
			seeder.NewGenericRecordGenerator(
				500,
				seeder.NewPatternStringGenerator(
					"%s:user:%s:profile",
					seeder.NewEnumStringGenerator("dev", "prod"),
					hexGenerator,
				),
				seeder.NewRandStringGenerator(100, 1000, '1', '0'),
				seeder.NewIntRangeGenerator(3600, 86400),
			),
			seeder.NewGenericRecordGenerator(
				500,
				seeder.NewPatternStringGenerator(
					"%s:friends:foobar:%s:profile",
					seeder.NewEnumStringGenerator("dev", "prod"),
					hexGenerator,
				),
				seeder.NewRandStringGenerator(100, 1000, '1', '0'),
				seeder.NewIntRangeGenerator(3600, 86400),
			),
		)
	},
}

func init() {
	rootCmd.AddCommand(fillCmd)
	fillCmd.Flags().IntVar(&cycles, "cycles", 1000, "Cycles count to perform")
}
