package merkle_script

import (
	"fmt"
	"io"
	"os"

	"github.com/KYVENetwork/ksync/types"
	"github.com/rs/zerolog"
)

type Pool struct {
	PoolID         int64 `mapstructure:"pool_id"`
	TargetBundleID int   `mapstructure:"target_bundle_id"`
}

type BundleInfo struct {
	Bundle   types.FinalizedBundle
	Runtime  string
	BundleId int
	PoolId   int
}

type MerkleRootEntry struct {
	BundleId int
	PoolId   int
	Hash     [32]byte
}

func MerkleLogger(moduleName string) zerolog.Logger {
	writer := io.MultiWriter(os.Stdout)
	customConsoleWriter := zerolog.ConsoleWriter{Out: writer}
	customConsoleWriter.FormatCaller = func(i interface{}) string {
		return "\x1b[36m[Merkle]\x1b[0m"
	}

	logger := zerolog.New(customConsoleWriter).With().Str("module", moduleName).Timestamp().Logger()
	return logger
}

// An MerkleRootQueue is a min-heap.
type MerkleRootQueue []MerkleRootEntry

func (h MerkleRootQueue) Len() int { return len(h) }
func (h MerkleRootQueue) Less(i, j int) bool {
	return h[i].BundleId < h[j].BundleId
}
func (h MerkleRootQueue) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MerkleRootQueue) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(MerkleRootEntry))
}

func (h *MerkleRootQueue) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func GetMerkleFileName(poolId int) string {
	return fmt.Sprintf("merkle_roots_pool_%v", poolId)
}

func GetPoolHeights(pools []Pool) map[int]int {
	poolHeights := map[int]int{}
	for _, pool := range pools {
		fileName := GetMerkleFileName(int(pool.PoolID))
		stats, err := os.Stat(fileName)

		if err != nil { // file does not exist
			continue
		}
		// we have one hash per bundle, one hash is 32 bytes big
		// bundleHeight = size / 32
		height := stats.Size() / 32
		poolHeights[int(pool.PoolID)] = int(height)
	}
	return poolHeights
}
