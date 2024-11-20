package commands

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/KYVENetwork/ksync/collectors/pool"
	"github.com/KYVENetwork/ksync/utils"
	m "github.com/KYVENetwork/merkle-script"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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

		var pools []m.Pool
		if err := viper.UnmarshalKey("pools", &pools); err != nil {
			log.Fatalf("Error unmarshalling pools: %v", err)
		}

		var wg sync.WaitGroup
		context, cancel := context.WithCancel(context.Background())

		workerCount := viper.GetInt("worker")
		bundleCh := make(chan m.BundleInfo, 16)
		errorCh := make(chan error)
		merkleEntryCh := make(chan m.MerkleRootEntry)

		poolHeights := m.GetPoolHeights(pools)

		// start a bundle info collector for each pool id
		for _, p := range pools {
			wg.Add(1)
			localPool := p
			go func() {
				defer wg.Done()
				poolResponse, err := pool.GetPoolInfo(chainRest, localPool.PoolID)
				if err != nil {
					log.Printf("failed to get pool info for pool %v: %v", localPool.PoolID, err)
					return
				}
				m.StartBundleIndexer(context, bundleCh, errorCh, chainRest, *poolResponse, localPool.TargetBundleID, poolHeights[int(localPool.PoolID)])
			}()
		}
		// spawn merkle worker that download all the bundles
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func() {
				defer wg.Done()
				m.StartBundleCollector(context, merkleEntryCh, bundleCh, errorCh, storageRest)
			}()
		}

		// start merkle writer
		go m.StartMerkleWriter(context, merkleEntryCh, errorCh, pools, cancel, poolHeights)

		// cancel the context if an error occured
		go func() {
			err := <-errorCh
			logger.Err(err).Msg("error occured")
			cancel()
		}()

		wg.Wait()
		logger.Info().Msg("finished")
	},
}
