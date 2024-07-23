package cmd

import (
	"fmt"
	"log"
	"regexp"
	"s3migration/migration"
	"s3migration/util"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	migrationDest string
	retryInterval string
	startAt       string
	endAt         string
	latestOnly    string
	startDt       time.Time
	endDt         time.Time
)

func init() {
	rootCmd.AddCommand(runCommand)

	runCommand.Flags().StringVar(&migrationDest, destinationBucketArgName, "", "Destination bucket name")
	runCommand.Flags().StringVar(&retryInterval, retryArgName, "1h", "[Optional] Retry duration if inventory not available, eg. 1h, 30m, 10s")
	runCommand.Flags().StringVar(&latestOnly, latestOnlyArgName, "", "[Optional] Copy only Latest/Non-latest version objects, eg. Yes/No")
	runCommand.Flags().StringVar(&startAt, startAtArgName, "", "[Optional] Start Datetime filter against object last updated date, eg '2023-09-30 12:00:00'")
	runCommand.Flags().StringVar(&endAt, endAtArgName, "", "[Optional] End Datetime filter against object last updated date, eg '2023-12-31 12:00:00'")
	runCommand.Flags().StringVar(&kmsID, kmsIDArgName, "SSE-S3", "[Optional] KMS key id")

	_ = runCommand.MarkFlagRequired(destinationBucketArgName)
}

var runCommand = &cobra.Command{
	Use:          "run",
	Short:        "Run S3 migration",
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := time.ParseDuration(retryInterval); err != nil {
			log.Fatalf("Invalid input param value '%s': '%s', error: %v", retryArgName, retryInterval, err)
		}
		var regSuccessThreshold float32 = 0.8
		migrationArgs := migration.MigrationArgs{
			SourceRegion:        sourceRegion,
			AccountID:           migrationAcctId,
			SourceBucket:        migrationSrc,
			RoleArn:             migrationRole,
			DestinationBucket:   migrationDest,
			RetryInterval:       retryInterval,
			ConfigName:          inventoryConfig,
			LatestOnly:          latestOnly,
			ReqSuccessThreshold: regSuccessThreshold,
			KmsID:               kmsID,
			Region:              sourceRegion,
			StartDt:             startDt,
			EndDt:               endDt,
		}
		if err := migration.Run(migrationArgs); err != nil {
			log.Fatal(err)
		}
		return nil
	},
	PreRunE: validateArgs,
}

func validateArgs(cmd *cobra.Command, args []string) error {
	// Validate latest-only flag
	if strings.TrimSpace(latestOnly) != "" {
		switch strings.ToUpper(latestOnly) {
		case "YES":
			latestOnly = util.IsLatestYes
		case "NO":
			latestOnly = util.IsLatestNo
		default:
			return fmt.Errorf("input arg '%s' value '%v' is not valid", latestOnlyArgName, latestOnly)
		}
	}
	// Validate date filters
	validateDateFlag := func(dtstr string) (time.Time, error) {
		if strings.TrimSpace(dtstr) != "" {
			return util.ParseDateTime(dtstr)
		}
		return time.Time{}, nil
	}
	var err error
	startDt, err = validateDateFlag(startAt)
	if err != nil {
		return fmt.Errorf("invalid '%s' date time arg value, '%v', valid date time formate is '%s'",
			startAtArgName, err.Error(), time.DateTime)
	}
	endDt, err = validateDateFlag(endAt)
	if err != nil {
		return fmt.Errorf("invalid '%s' date time arg value, '%v', valid date time formate is '%s'",
			endAtArgName, err.Error(), time.DateTime)

	}

	// AccountID validation
	if ok, _ := regexp.MatchString(`\d{12}`, migrationAcctId); !ok {
		return fmt.Errorf("invalid '%s' arg value '%v', it must be [12] digit number", accountIdArgName, migrationAcctId)
	}

	//  Role ARN validation=
	if ok, _ := regexp.MatchString(`^(?:\d{12}|(arn:(aws|aws-us-gov|aws-cn):iam::\d{12}(?:|:(?:role\/[0-9A-Za-z\+\.@_,-]{1,64}))))$`, migrationRole); !ok {
		return fmt.Errorf("invalid '%s' arg value '%v'. it must be an AWS ARN eg. arn:aws:iam::<ACCOUNT_NUM>:role/BatchOperationsCopyRole", roleArgName, migrationRole)
	}

	return nil
}
