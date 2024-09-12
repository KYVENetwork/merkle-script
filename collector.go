package merkle_script

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/ksync/collectors/bundles"
	"github.com/KYVENetwork/ksync/types"
	"github.com/KYVENetwork/ksync/utils"
	"strconv"
	"time"
)

var logger = MerkleLogger("pool")

func StartBundleCollector(merkleCh chan<- []MerkleRootEntry, errorCh chan<- error, chainRest, storageRest string, pool types.PoolResponse, runtime string, targetBundleId int) {
	paginationKey := ""
	for {
		var merkleRoots []MerkleRootEntry

		bundlesPage, nextKey, err := bundles.GetFinalizedBundlesPage(chainRest, pool.Pool.Id, 10, paginationKey, false)
		if err != nil {
			errorCh <- fmt.Errorf("failed to get finalized bundles page: %w", err)
			return
		}

		for _, finalizedBundle := range bundlesPage {
			deflated, err := bundles.GetDataFromFinalizedBundle(finalizedBundle, storageRest)
			if err != nil {
				errorCh <- fmt.Errorf("failed to get data from finalized bundle: %w", err)
				return
			}

			// parse bundle
			var bundle types.Bundle

			if err := json.Unmarshal(deflated, &bundle); err != nil {
				errorCh <- fmt.Errorf("failed to unmarshal tendermint bundle: %w", err)
				return
			}

			bundleId, err := strconv.Atoi(finalizedBundle.Id)
			if err != nil {
				errorCh <- fmt.Errorf("failed to convert ID from finalized bundle to string: %w", err)
				return
			}

			var leafHashes [][32]byte

			leafHashes = BundleToHashes(bundle, runtime)

			merkleRoot := GenerateMerkleRoot(&leafHashes)

			logger.Info().
				Int("bundle-id", bundleId).
				Int64("pool-id", pool.Pool.Id).
				Str("root", hex.EncodeToString(merkleRoot[:])).
				Msg("computed Merkle root")

			merkleRoots = append(merkleRoots, MerkleRootEntry{
				BundleId:   bundleId,
				MerkleRoot: merkleRoot,
			})

			if targetBundleId != 0 && bundleId >= targetBundleId {
				logger.Info().
					Int("target-height", targetBundleId).
					Msg("reached target bundle")
				merkleCh <- merkleRoots
				return
			}
		}
		merkleCh <- merkleRoots

		if nextKey == "" {
			// if there is no new page we do not continue
			panic("reached latest bundle on pool, target bundle ID was higher than latest pool bundle")
		}

		time.Sleep(utils.RequestTimeoutMS)
		paginationKey = nextKey
	}
}
