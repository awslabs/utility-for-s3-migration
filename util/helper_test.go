package util

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
	s3ctrtypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
)

func TestGetQueryExpression(t *testing.T) {
	useCases := []struct {
		testName           string
		fileSchema         string
		startDt            time.Time
		endDt              time.Time
		latestOnly         string
		versioningDisabled bool
	}{
		{
			testName:           "Default behaviour with all parameter correct",
			fileSchema:         "Bucket, Key, VersionId, IsLatest, IsDeleteMarker,LastUpdated",
			startDt:            time.Now().AddDate(0, 0, 1),
			endDt:              time.Now(),
			latestOnly:         "Yes",
			versioningDisabled: false,
		},
		{
			testName:           "Missing LastUpdated file schema column.",
			fileSchema:         "Bucket, Key, VersionId, IsLatest, IsDeleteMarker",
			startDt:            time.Now().AddDate(0, 0, 1),
			endDt:              time.Now(),
			latestOnly:         "No",
			versioningDisabled: false,
		},
		{
			testName:           "Vesrioning is disable.",
			fileSchema:         "Bucket, Key, VersionId, IsLatest, IsDeleteMarker",
			startDt:            time.Now().AddDate(0, 0, 1),
			endDt:              time.Now(),
			latestOnly:         "No",
			versioningDisabled: true,
		},
		{
			testName:           "Only End Date",
			fileSchema:         "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, LastUpdated",
			endDt:              time.Now(),
			latestOnly:         "No",
			versioningDisabled: false,
		},
		{
			testName:           "Only Start Date",
			fileSchema:         "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, LastUpdated",
			startDt:            time.Now().AddDate(0, 0, 1),
			latestOnly:         "No",
			versioningDisabled: false,
		},
	}

	for _, uCase := range useCases {
		t.Run(uCase.testName, func(t *testing.T) {
			q, err := GetQueryExpression(uCase.fileSchema, uCase.startDt, uCase.endDt, uCase.latestOnly, uCase.versioningDisabled)
			if err != nil {
				t.Errorf("got  error %s, want nil", err.Error())
			}
			if q != "" {
				fmt.Printf("\n OUTPUT: %s", q)
			}
		})
	}

}

func TestGetJobSuccessThreshold(t *testing.T) {
	// Initialize a logger for testing
	// logger, _ := zap.NewDevelopment()
	// zap.ReplaceGlobals(logger)

	testCases := []struct {
		name     string
		jobs     []*s3control.DescribeJobOutput
		expected float32
	}{
		{
			name:     "NoJobs",
			jobs:     nil,
			expected: 0,
		},
		{
			name: "SingleJobWithZeroTasks",
			jobs: []*s3control.DescribeJobOutput{
				{
					Job: &s3ctrtypes.JobDescriptor{
						JobArn: aws.String("test"),
						JobId:  aws.String("test"),
						ProgressSummary: &s3ctrtypes.JobProgressSummary{
							TotalNumberOfTasks:     aws.Int64(0),
							NumberOfTasksSucceeded: aws.Int64(0),
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "SingleJobWithAllTasksSucceeded",
			jobs: []*s3control.DescribeJobOutput{
				{
					Job: &s3ctrtypes.JobDescriptor{
						JobArn: aws.String("test"),
						JobId:  aws.String("test"),
						ProgressSummary: &s3ctrtypes.JobProgressSummary{
							TotalNumberOfTasks:     aws.Int64(10),
							NumberOfTasksSucceeded: aws.Int64(10),
						},
					},
				},
			},
			expected: 1,
		},
		{
			name: "MultipleJobsWithVaryingSuccessRates",
			jobs: []*s3control.DescribeJobOutput{
				{
					Job: &s3ctrtypes.JobDescriptor{
						JobArn: aws.String("test"),
						JobId:  aws.String("test"),
						ProgressSummary: &s3ctrtypes.JobProgressSummary{
							TotalNumberOfTasks:     aws.Int64(10),
							NumberOfTasksSucceeded: aws.Int64(8),
						},
					},
				},
				{
					Job: &s3ctrtypes.JobDescriptor{
						JobArn: aws.String("test"),
						JobId:  aws.String("test"),
						ProgressSummary: &s3ctrtypes.JobProgressSummary{
							TotalNumberOfTasks:     aws.Int64(20),
							NumberOfTasksSucceeded: aws.Int64(15),
						},
					},
				},
			},
			expected: 0.75,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetJobSuccessThreshold(tc.jobs...)
			if result != tc.expected {
				t.Errorf("GetJobSuccessThreshold() = %f, expected %f", result, tc.expected)
			}
		})
	}
}

func TestParseDateTime(t *testing.T) {
	invalidUseCases := []struct {
		testName string
		dateStr  string
	}{
		{
			testName: "Empty Date String",
			dateStr:  "",
		},
		{
			testName: "In-valid Date String",
			dateStr:  "!@#@!#Aasdew",
		},
		{
			testName: "In-valid Date Format",
			dateStr:  "2023-12-31T12:00:00",
		},
		{
			testName: "In-valid Date String 2",
			dateStr:  "2023-1-31 17:00:00",
		},
	}

	validUseCases := []struct {
		testName string
		dateStr  string
	}{
		{
			testName: "Valid Date String 1",
			dateStr:  "2023-12-31 12:00:00",
		},
		{
			testName: "Valid Date String 2",
			dateStr:  "2023-01-31 17:00:00",
		},
	}
	for _, uCase := range invalidUseCases {
		t.Run(uCase.testName, func(t *testing.T) {
			_, err := ParseDateTime(uCase.dateStr)
			if err == nil {
				t.Errorf("got  nil , want error")
			}
		})
	}
	for _, uCase := range validUseCases {
		t.Run(uCase.testName, func(t *testing.T) {
			dt, err := ParseDateTime(uCase.dateStr)
			if err != nil {
				t.Errorf("got  error %s, want nil", err.Error())
			}
			fmt.Printf("\n OUTPUT: %s", dt)
		})
	}

}
