package commands

import (
	"fmt"
	"github.com/KYVENetwork/ksync/collectors/pool"
	"github.com/KYVENetwork/ksync/utils"
	m "github.com/KYVENetwork/merkle-script"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

type Pool struct {
	PoolID         int64 `mapstructure:"pool_id"`
	TargetBundleID int   `mapstructure:"target_bundle_id"`
}

var (
	logger = m.MerkleLogger("cmd")
)

func init() {
	startCmd.Flags().StringVar(&config, "config", "./", "config file path")

	startCmd.Flags().StringVar(&storageRest, "storage-rest", "", "storage endpoint for requesting bundle data")

	rootCmd.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start merkle-script to create historical Merkle roots",
	Run: func(cmd *cobra.Command, args []string) {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(config)
		err := viper.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("config file error: %w", err))
		}

		chainRest := utils.GetChainRest(viper.GetString("chain_id"), viper.GetString("chain_rest"))
		storageRest = strings.TrimSuffix(storageRest, "/")

		var pools []Pool
		if err := viper.UnmarshalKey("pools", &pools); err != nil {
			log.Fatalf("Error unmarshalling pools: %v", err)
		}

		var wg sync.WaitGroup

		for _, p := range pools {
			wg.Add(1)
			go func(p Pool) {
				poolResponse, err := pool.GetPoolInfo(chainRest, p.PoolID)
				if err != nil {
					log.Printf("failed to get pool info for pool %v: %v", p.PoolID, err)
					return
				}

				merkleCh := make(chan []m.MerkleRootEntry, utils.BlockBuffer)
				errorCh := make(chan error)

				go func() {
					m.StartBundleCollector(merkleCh, errorCh, chainRest, storageRest, *poolResponse, poolResponse.Pool.Data.Runtime, p.TargetBundleID)
					close(merkleCh)
					close(errorCh)
				}()

				go func() {
					defer wg.Done()

					for {
						select {
						case err, ok := <-errorCh:
							if !ok {
								return
							}
							log.Printf("error in bundle collector for pool %v: %v", p.PoolID, err)
							return
						case merkleRoots, ok := <-merkleCh:
							if !ok {
								return
							}
							logger.Info().Int64("pool-id", p.PoolID).Msg("received merkle roots")

							sort.Slice(merkleRoots, func(i, j int) bool {
								return merkleRoots[i].BundleId < merkleRoots[j].BundleId
							})

							file, err := os.OpenFile(fmt.Sprintf("merkle_roots_pool_%v", p.PoolID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
							if err != nil {
								log.Printf("error creating file for pool %v: %v", p.PoolID, err)
								return
							}
							defer file.Close()

							for _, entry := range merkleRoots {
								_, err := file.Write(entry.MerkleRoot[:])
								if err != nil {
									log.Printf("error writing to file for pool %v: %v", p.PoolID, err)
									return
								}
							}
							logger.Info().Msg(fmt.Sprintf("finished writing merkle roots for pool-id %v", p.PoolID))
						}
					}
				}()
			}(p)
		}
		wg.Wait()
	},
}
