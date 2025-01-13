package merkle_script

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/ksync/types"
)

type Integration interface {
	BundleToHashes() [][32]byte
	dataItemToSha256() [32]byte
}

type TendermintValue struct {
	Block        json.RawMessage `json:"block"`
	BlockResults json.RawMessage `json:"block_results"`
}

// GenerateMerkleRoot computes a Merkle root based on given hashes recursively.
func GenerateMerkleRoot(hashes *[][32]byte) [32]byte {
	// Verifies that the number of hashes is equal.
	// Duplicate the last hash if it's unequal.
	if len(*hashes)%2 == 1 {
		*hashes = append(*hashes, (*hashes)[len(*hashes)-1])
	}

	var combinedHashes [][32]byte

	// Calculate parent leaf by hashing left and right leaf hash.
	for i := 0; i < len(*hashes); i += 2 {
		left := (*hashes)[i]
		right := (*hashes)[i+1]
		combined := append(left[:], right[:]...)
		parentHash := sha256.Sum256(combined)
		combinedHashes = append(combinedHashes, parentHash)
	}

	if len(combinedHashes) == 1 {
		return combinedHashes[0]
	}
	return GenerateMerkleRoot(&combinedHashes)
}

func BundleToHashes(bundle types.Bundle, runtime string) [][32]byte {
	var leafHashes [][32]byte

	switch runtime {
	case "@kyvejs/tendermint-bsync":
		for _, dataItem := range bundle {
			leafHashes = append(leafHashes, sha256.Sum256([]byte(fmt.Sprintf("{\"key\":\"%s\",\"value\":%s}", dataItem.Key, string(dataItem.Value)))))
		}
	case "@kyvejs/tendermint":
		for _, dataItem := range bundle {
			leafHashes = append(leafHashes, dataItemToSha256(dataItem))
		}
	default:
		logger.Error().
			Str("runtime", runtime).
			Msg("runtime not supported")
		panic("runtime not supported")
	}

	return leafHashes
}

func dataItemToSha256(dataItem types.DataItem) [32]byte {
	merkleRoot := createHashesForTendermintValue(dataItem)

	keyBytes := sha256.Sum256([]byte(dataItem.Key))

	combined := append(keyBytes[:], merkleRoot[:]...)

	return sha256.Sum256(combined)
}

func createHashesForTendermintValue(dataItem types.DataItem) [32]byte {
	var tendermintValue TendermintValue

	if err := json.Unmarshal(dataItem.Value, &tendermintValue); err != nil {
		panic(err)
	}

	var hashes [][32]byte

	hashes = append(hashes, sha256.Sum256(tendermintValue.Block))
	hashes = append(hashes, sha256.Sum256(tendermintValue.BlockResults))

	return GenerateMerkleRoot(&hashes)
}
