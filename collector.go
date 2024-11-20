package merkle_script

import (
	"container/heap"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/KYVENetwork/ksync/collectors/bundles"
	"github.com/KYVENetwork/ksync/types"
	"github.com/KYVENetwork/ksync/utils"
)

var logger = MerkleLogger("pool")

// Bundle Indexer
// fetches all the finalized bundles from the specified pool to the target height
// error will be put into the error channel
// bundles are inserted in-order
func StartBundleIndexer(context context.Context, bundleCh chan<- BundleInfo, errorCh chan<- error, chainRest string, pool types.PoolResponse, targetBundleId int, startHeight int) {
	paginationKey := ""
	for {
		bundlesPage, nextKey, err := bundles.GetFinalizedBundlesPage(chainRest, pool.Pool.Id, 10, paginationKey, false)
		if err != nil {
			errorCh <- fmt.Errorf("failed to get finalized bundles page: %w", err)
			return
		}

		for _, bundle := range bundlesPage {
			// return if the context is closed

			bundleId, err := strconv.Atoi(bundle.Id)

			// dont append already indexed bundles
			if bundleId < startHeight {
				continue
			}

			if err != nil {
				errorCh <- fmt.Errorf("failed to convert ID from finalized bundle to string: %w", err)
				return
			}
			if targetBundleId != 0 && bundleId >= targetBundleId {
				logger.Info().
					Int("target-height", targetBundleId).
					Msg("reached target bundle")
				return
			}

			bundleInfo := BundleInfo{Bundle: bundle, PoolId: int(pool.Pool.Id), Runtime: pool.Pool.Data.Runtime, BundleId: bundleId}
			select {
			case <-context.Done():
				return
			case bundleCh <- bundleInfo:
				continue
			}
		}

		if nextKey == "" {
			// if there is no new page we do not continue
			panic("reached latest bundle on pool, target bundle ID was higher than latest pool bundle")
		}

		time.Sleep(utils.RequestTimeoutMS)
		paginationKey = nextKey
	}
}

// the bundle collector downloads and unpacks the bundles data,
// calculates the merkle hash and puts it into the merkle entry channel
func StartBundleCollector(context context.Context, merkleEntries chan<- MerkleRootEntry, bundleCh <-chan BundleInfo, errorCh chan<- error, storageRest string) {
	for {

		select {
		case <-context.Done():
			return
		case bundle := <-bundleCh:
			decompressedBundle, err := bundles.GetDataFromFinalizedBundle(bundle.Bundle, storageRest)

			if err != nil {
				logger.Info().
					Str("bundleId", bundle.Bundle.Id).
					Int("poolId", bundle.PoolId).
					Msg("error while fetching")
				errorCh <- err
				return
			}

			// parse bundle
			var dataItems []types.DataItem

			if err := json.Unmarshal(decompressedBundle, &dataItems); err != nil {
				errorCh <- fmt.Errorf("failed to unmarshal tendermint bundle: %w", err)
				return
			}

			leafHashes := BundleToHashes(dataItems, bundle.Runtime)
			merkleRoot := GenerateMerkleRoot(&leafHashes)

			merkleEntry := MerkleRootEntry{BundleId: bundle.BundleId, Hash: merkleRoot, PoolId: bundle.PoolId}
			select {
			case <-context.Done():
				return
			case merkleEntries <- merkleEntry:
				logger.Info().
					Int("bundleId", merkleEntry.BundleId).
					Int("poolId", merkleEntry.PoolId).
					Str("root", hex.EncodeToString(merkleEntry.Hash[:])).
					Msg("computed Merkle root")
				continue
			}
		}
	}
}

// the merkle writer collects all the merkle entries for each pool and appends them in order, so that no merkle entry is missing
func StartMerkleWriter(context context.Context, merkleEntries <-chan MerkleRootEntry, errorCh chan<- error, pools []Pool, cancel context.CancelFunc, poolHeights map[int]int) {

	poolEntries := map[int]*MerkleRootQueue{}
	if reachedTargetHeight(poolHeights, pools) {
		cancel()
		return
	}

	for {
		select {
		case <-context.Done():
			return
		case entry := <-merkleEntries:
			// insert the entry into the priority queue
			queue := poolEntries[entry.PoolId]
			if queue == nil {
				queue = &MerkleRootQueue{}
				poolEntries[entry.PoolId] = queue
			}
			heap.Push(queue, entry)
			// write all hashes that are in order and come next
			for queue.Len() > 0 && (*queue)[0].BundleId == poolHeights[entry.PoolId] {
				entry := heap.Pop(queue).(MerkleRootEntry)
				poolHeights[entry.PoolId]++
				appendMerkleRoot(entry)
				logger.Info().Int("height", poolHeights[entry.PoolId]).Int("pool", entry.PoolId).Msg("writing hashes")

				if reachedTargetHeight(poolHeights, pools) {
					cancel()
					return
				}
			}
		}
	}
}

func reachedTargetHeight(poolHeights map[int]int, pools []Pool) bool {
	var reachedHeight = true
	for _, targetPool := range pools {
		if poolHeights[int(targetPool.PoolID)] != targetPool.TargetBundleID {
			reachedHeight = false
		}
	}
	return reachedHeight
}

func appendMerkleRoot(entry MerkleRootEntry) {
	merkleFileName := GetMerkleFileName(entry.PoolId)
	file, err := os.OpenFile(merkleFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Err(err).Msg("error while opening file.")
		panic(1)
	}
	defer file.Close()
	_, err = file.Write(entry.Hash[:])
	if err != nil {
		logger.Err(err).Msg("error while writing to file.")
		panic(1)
	}

}
