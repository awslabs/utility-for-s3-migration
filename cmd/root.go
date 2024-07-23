package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Define constants for the argument names for all subcommands
const (
	regionArgName            = "region"
	sourceBucketArgName      = "sourcebucket"
	destinationBucketArgName = "destinationbucket"
	accountIdArgName         = "account"
	roleArgName              = "role"
	retryArgName             = "retry"
	inventoryConfigArgName   = "inventoryconfig"
	localInventoryArgName    = "local-inventory"
	startAtArgName           = "start"
	endAtArgName             = "end"
	latestOnlyArgName        = "latest-only"
	kmsIDArgName             = "kms-id"
)

// Persistent argument values
var (
	sourceRegion    string
	migrationAcctId string
	migrationSrc    string
	migrationRole   string
	inventoryConfig string
	kmsID           string
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&sourceRegion, regionArgName, "", "AWS region to operate in")
	rootCmd.PersistentFlags().StringVar(&migrationSrc, sourceBucketArgName, "", "source bucket name")
	rootCmd.PersistentFlags().StringVar(&migrationAcctId, accountIdArgName, "", "AWS account ID where S3 Batch job will run (typically account with source bucket)")
	rootCmd.PersistentFlags().StringVar(&migrationRole, roleArgName, "", "Role for batch operation to access cross account bucket")
	rootCmd.PersistentFlags().StringVar(&inventoryConfig, inventoryConfigArgName, "bulk-copy-inventory", "Name of inventory configuration")

	_ = rootCmd.MarkPersistentFlagRequired(regionArgName)
	_ = rootCmd.MarkPersistentFlagRequired(sourceBucketArgName)
	_ = rootCmd.MarkPersistentFlagRequired(accountIdArgName)
	_ = rootCmd.MarkPersistentFlagRequired(roleArgName)
}

func initConfig() {}

var rootCmd = &cobra.Command{
	Use:              "s3-migration",
	Short:            "Performs S3 cross-account/same-account copy using S3 Batch job operations",
	TraverseChildren: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
