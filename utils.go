package merkle_script

import (
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"os"
)

type MerkleRootEntry struct {
	BundleId   int
	MerkleRoot [32]byte
}

// CheckConsecutiveBundleIds verifies that an array of MerkleRoot entries is consecutive.
func CheckConsecutiveBundleIds(entries []MerkleRootEntry) error {
	for i := 1; i < len(entries); i++ {
		if entries[i].BundleId != entries[i-1].BundleId+1 {
			return fmt.Errorf("bundleIds are not consecutive")
		}
	}
	return nil
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
