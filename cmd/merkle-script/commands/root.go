package commands

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	config      string
	storageRest string
)

var rootCmd = &cobra.Command{
	Use:   "merkle-script",
	Short: "merkle-script can be used to reconstruct the Merkle root for already archived bundles.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(fmt.Errorf("failed to execute root command: %w", err))
	}
}
