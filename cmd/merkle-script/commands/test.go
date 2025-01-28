package commands

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/ksync/collectors/bundles"
	"github.com/KYVENetwork/ksync/types"
	"github.com/KYVENetwork/ksync/utils"
	m "github.com/KYVENetwork/merkle-script"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

func init() {
	testCmd.Flags().Int64Var(&bundleId, "bundle-id", 0, "bundle id")

	testCmd.Flags().Int64Var(&poolId, "pool-id", 0, "pool id")

	rootCmd.AddCommand(testCmd)
}

var testCmd = &cobra.Command{
	Use:   "test",
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

		bundlesPage, _, err := bundles.GetFinalizedBundlesPageWithOffset(chainRest, poolId, 1, bundleId, "", false)
		if err != nil {
			panic(err)
			return
		}

		deflated, err := bundles.GetDataFromFinalizedBundle(bundlesPage[0], storageRest)
		if err != nil {
			panic(err)
			return
		}

		var bundle types.Bundle

		if err := json.Unmarshal(deflated, &bundle); err != nil {
			panic(err)
			return
		}

		bundleId, err := strconv.Atoi(bundlesPage[0].Id)
		if err != nil {
			panic(err)
			return
		}

		var leafHashes [][32]byte

		leafHashes = m.BundleToHashes(bundle, "@kyvejs/tendermint")

		merkleRoot := m.GenerateMerkleRoot(&leafHashes)

		logger.Info().
			Int("bundle-id", bundleId).
			Str("root", hex.EncodeToString(merkleRoot[:])).
			Msg("computed Merkle root")
	},
}
