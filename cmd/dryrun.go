package cmd

import (
	"log"
	"s3migration/migration"

	"github.com/spf13/cobra"
)

// Subcommand argument values
var (
	localInventoryFile string
)

func init() {
	rootCmd.AddCommand(dryRunCommand)
	dryRunCommand.Flags()
	dryRunCommand.Flags().StringVar(&localInventoryFile, localInventoryArgName, "", "Destination bucket name")
}

var dryRunCommand = &cobra.Command{
	Use:          "dry-run",
	Short:        "Dry Run S3 migration, it validates the required setting to run the actual operation",
	SilenceUsage: false,
	Run: func(cmd *cobra.Command, args []string) {
		if err := migration.DryRun(sourceRegion, migrationAcctId, migrationSrc, migrationRole, inventoryConfig, localInventoryFile); err != nil {
			log.Fatal(err)
		}
	},
	TraverseChildren: true,
}
