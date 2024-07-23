package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"s3migration/util"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
	s3controltypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

func init() {
	logger := zap.Must(zap.NewProduction())
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		logger = zap.Must(zap.NewDevelopment())
	}
	zap.ReplaceGlobals(logger)
}

const (
	inventoryConfigName = "bulk-copy-inventory"
)

type s3migration struct {
	s3Client    s3API
	s3CtrClient s3ControlAPI
}

func (s3obj *s3migration) ensureS3InventoryConfig(ctx context.Context, bucket string, configName string, shouldUpdate bool) (*inventoryManifestFinderArgs, error) {
	out, err := s3obj.s3Client.GetBucketInventoryConfiguration(ctx, &s3.GetBucketInventoryConfigurationInput{
		Bucket: aws.String(bucket),
		Id:     aws.String(configName),
	})

	zap.L().Debug("Checking for presence of inventory configuration",
		zap.String("bucket", bucket),
		zap.String("configName", configName),
		zap.Bool("shouldUpdate", shouldUpdate),
	)

	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			// NoSuchConfiguration is expected if the config does not exist.
			if ae.ErrorCode() != "NoSuchConfiguration" {
				return nil, err
			}
			// If we got a non-default inventory config name and it doesn't exist, bail
			if !shouldUpdate {
				zap.L().Error("Non-default inventory config does not exist")
				return nil, err
			}
		}
	}

	// The non-default configuration doesn't belong to us.  If it happens to be disabled, then
	// fail rather than modify something that isn't ours.
	if out != nil && !*out.InventoryConfiguration.IsEnabled && !shouldUpdate {
		return nil, fmt.Errorf("non-default inventory config %s is disabled", configName)
	}

	prefix := fmt.Sprintf("%s/%s/", bucket, configName)
	// If configuration exists and is enabled, no further work required
	if out != nil && *out.InventoryConfiguration.IsEnabled {
		dateWindow := -1
		if out.InventoryConfiguration.Schedule.Frequency == s3types.InventoryFrequencyWeekly {
			dateWindow = -8
		}
		destinationArn := *out.InventoryConfiguration.Destination.S3BucketDestination.Bucket
		if out.InventoryConfiguration.Destination.S3BucketDestination.Prefix != nil {
			prefix = fmt.Sprintf("%s/%s", *out.InventoryConfiguration.Destination.S3BucketDestination.Prefix, prefix)
		}
		return &inventoryManifestFinderArgs{
			BucketName: destinationArn[strings.LastIndex(destinationArn, ":")+1:],
			Prefix:     prefix,
			DateWindow: dateWindow,
		}, nil
	}
	zap.L().Info("Inventory configuration does not exist or is disabled.  Creating/enabling",
		zap.String("bucket", bucket),
		zap.String("configName", configName),
	)

	// Create/Update configuration
	_, err = s3obj.s3Client.PutBucketInventoryConfiguration(ctx, &s3.PutBucketInventoryConfigurationInput{
		Bucket: aws.String(bucket),
		Id:     aws.String(inventoryConfigName),
		InventoryConfiguration: &s3types.InventoryConfiguration{
			Destination: &s3types.InventoryDestination{
				S3BucketDestination: &s3types.InventoryS3BucketDestination{
					Bucket: util.GetArn(bucket),
					Encryption: &s3types.InventoryEncryption{
						SSES3: &s3types.SSES3{},
					},
					Format: s3types.InventoryFormatCsv,
				},
			},
			Id:                     aws.String(inventoryConfigName),
			IncludedObjectVersions: s3types.InventoryIncludedObjectVersionsAll,
			IsEnabled:              aws.Bool(true),
			Schedule: &s3types.InventorySchedule{
				Frequency: s3types.InventoryFrequencyDaily,
			},
			OptionalFields: []s3types.InventoryOptionalField{
				s3types.InventoryOptionalFieldLastModifiedDate,
				s3types.InventoryOptionalFieldReplicationStatus,
				s3types.InventoryOptionalFieldSize, // Batch operations has a 5GB limit, can use this to filter those out
			},
		},
	})

	// These are the same values set above
	return &inventoryManifestFinderArgs{
		BucketName: bucket,
		Prefix:     prefix,
		DateWindow: -1,
	}, err
}

func (s3obj *s3migration) getLatestManifest(ctx context.Context, finderArgs *inventoryManifestFinderArgs) (*s3types.Object, error) {
	windowStart := time.Now().Add(time.Duration(finderArgs.DateWindow) * time.Hour * 48)
	// expected prefix for inventory manifests
	dateString := windowStart.Format("2006-01-02")
	startAfter := fmt.Sprintf("%s%s", finderArgs.Prefix, dateString)

	// List objects in the bucket
	out, err := s3obj.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(finderArgs.BucketName),
		Prefix:     aws.String(finderArgs.Prefix),
		StartAfter: aws.String(startAfter),
	})
	// if we get an error or ListObjectsV2 doesn't return any results?
	if err != nil {
		zap.L().Fatal("call to ListObjectsV2 failed", zap.Error(err))
	}

	zap.L().Debug("ListObjectsV2 complete",
		zap.String("bucket", finderArgs.BucketName),
		zap.String("prefix", finderArgs.Prefix),
		zap.String("startAfter", startAfter),
		zap.Int("count", len(out.Contents)),
	)

	manifests := []s3types.Object{}
	for _, obj := range out.Contents {
		// Check if the object is a manifest and is within our time frame window
		if strings.HasSuffix(*obj.Key, "manifest.json") && obj.LastModified.After(windowStart) {
			manifests = append(manifests, obj)
		}
	}

	if len(manifests) == 0 {
		zap.L().Info("No manifest file available",
			zap.String("prefix", finderArgs.Prefix),
			zap.String("date", dateString),
		)
		return nil, nil
	}
	slices.SortFunc(manifests, objectDateDescending)

	return &manifests[0], nil
}

func (s3obj *s3migration) isVersioningDisabled(ctx context.Context, bucket string) (bool, error) {
	out, err := s3obj.s3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return out.Status == "", nil
}

func (s3obj *s3migration) readInventoryManifest(ctx context.Context, bucket string, manifest s3types.Object) (*manifestJson, error) {
	// Get manifest
	out, err := s3obj.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(*manifest.Key),
	})
	if err != nil {
		return &manifestJson{}, err
	}
	// Read manifest object to string and unmarshal JSON
	defer out.Body.Close()
	var manifestContent manifestJson
	body, err := io.ReadAll(out.Body)
	if err != nil {
		zap.L().Fatal("error reading inventory manifest.json", zap.Error(err))
	}

	if err := json.Unmarshal(body, &manifestContent); err != nil {
		zap.L().Fatal("inventory manifest.json is corrupt or malformed", zap.Error(err))
	}

	return &manifestContent, nil
}

// Use S3 Select to get just the bucket and key from a gzipped CSV generated by the inventory process
func (s3obj *s3migration) filterManifestCsv(ctx context.Context, args *batchJobArgs,
	manifest s3types.Object, filters userFilters) (*s3types.Object, error) {
	manifestJson, err := s3obj.readInventoryManifest(ctx, *args.SourceBucketName, manifest)
	if err != nil {
		return &s3types.Object{}, err
	}

	csvFile := manifestJson.Files[0].Key
	zap.L().Info("Processing existing inventory datafile",
		zap.String("csvFile", csvFile),
	)

	bucketAndKeyExpression, err := util.GetQueryExpression(manifestJson.FileSchema, filters.StartDate,
		filters.EndDate, filters.LatestOnly, args.VersioningDisabled)
	if err != nil {
		return nil, err
	}
	rdr := s3obj.filterGzippedCsv(ctx, *args.SourceBucketName, csvFile, bucketAndKeyExpression)

	// The filtered data file will have a similar name to the automatically generated data file.
	// However, as we're expecting a gzipped file and are uploading an uncompressed file, we trim the ".gz" from the key
	key := strings.TrimSuffix(csvFile, ".gz")
	return s3obj.uploadS3File(ctx, *args.SourceBucketName, key, rdr)
}

// Execute the given S3 Select expression against provided bucket and key, returning an io.Reader wrapper
func (s3obj *s3migration) filterGzippedCsv(ctx context.Context, bucket, key, expression string) *util.S3SelectReader {
	out, err := s3obj.s3Client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		Expression:     aws.String(expression),
		ExpressionType: s3types.ExpressionTypeSql,
		InputSerialization: &s3types.InputSerialization{
			CSV: &s3types.CSVInput{
				FieldDelimiter: aws.String(","),
				FileHeaderInfo: s3types.FileHeaderInfoNone,
			},
			CompressionType: s3types.CompressionTypeGzip,
		},
		RequestProgress: &s3types.RequestProgress{Enabled: aws.Bool(false)},
		OutputSerialization: &s3types.OutputSerialization{
			CSV: &s3types.CSVOutput{},
		},
	})
	if err != nil {
		zap.L().Fatal("Error filtering CSV file with S3 Select",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.String("expression", expression),
			zap.Error(err),
		)
	}
	return &util.S3SelectReader{Stream: out.GetStream()}
}

func (s3obj s3migration) uploadS3File(ctx context.Context, bucket, key string, reader io.Reader) (*s3types.Object, error) {
	// The s3 manager feature is being used as we don't have a Content-Length value for a direct PutObject.
	// The files being uploaded should not be very large, so we're configuring the uploader to minimize local resource usage
	uploader := manager.NewUploader(s3obj.s3Client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.LeavePartsOnError = false
		u.PartSize = 64 * 1024 * 1024 // 64MB per part.  Per docs, the minimum this can be is 5MB
	})

	result, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 reader,
		ServerSideEncryption: s3types.ServerSideEncryptionAes256,
	})

	if err != nil {
		zap.L().Fatal("failed to upload filtered inventory file",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.Error(err),
		)
	}
	zap.L().Info("Uploaded filtered inventory file",
		zap.String("Url", result.Location),
	)

	out, herr := s3obj.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if herr != nil {
		zap.L().Fatal("failed to get ETag for uploaded file",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.Error(herr),
		)
	}

	// Return Etag and key as bucket file name
	return &s3types.Object{
		ETag: aws.String(*out.ETag),
		Key:  aws.String(key),
	}, nil
}

// If bucket ownership is set to enforced, then copy operations with an ACL will fail.
// as per the AWS docs, the workaround is to submit a copy request with an ACL of "bucket-owner-full-control"
func (s3obj *s3migration) isOwnershipEnforced(ctx context.Context, bucket string) (bool, error) {
	out, err := s3obj.s3Client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return false, err
	}
	for _, rule := range out.OwnershipControls.Rules {
		if rule.ObjectOwnership == s3types.ObjectOwnershipBucketOwnerEnforced {
			return true, nil
		}
	}
	return false, nil
}

func Run(args MigrationArgs) error {
	defer util.ZapLogSync()
	ctx := context.Background()

	// get aws configuration from loacal aws credentials
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(args.SourceRegion))
	if err != nil {
		zap.L().Fatal(
			"Failed to load AWS client config",
			zap.String("region", args.SourceRegion),
			zap.Error(err),
		)
	}
	s3mig := &s3migration{s3Client: s3.NewFromConfig(cfg), s3CtrClient: s3control.NewFromConfig(cfg)}
	versioningDisabled, verr := s3mig.isVersioningDisabled(ctx, args.SourceBucket)
	if verr != nil {
		zap.L().Fatal("Failed to get versioning status", zap.Error(verr))
	}
	zap.L().Info("Bucket versioning status",
		zap.String("bucket", args.SourceBucket),
		zap.Bool("disabled", versioningDisabled),
	)
	shouldUpdate := args.ConfigName == inventoryConfigName
	manifestArgs, invErr := s3mig.ensureS3InventoryConfig(ctx, args.SourceBucket, args.ConfigName, shouldUpdate)
	if invErr != nil {
		zap.L().Fatal("Failed to get inventory config", zap.Error(invErr))
	}
	zap.L().Debug("Search criteria for latest inventory manifest",
		zap.String("bucket", manifestArgs.BucketName),
		zap.String("prefix", manifestArgs.Prefix),
		zap.Int("dateWindow", manifestArgs.DateWindow),
	)

	var (
		manifestFile *s3types.Object
		merr         error
		ctr          int16
	)

	// Try to get s3 bucket manifest details or wait until inventory file is available.
	// it is set to wait up to 24 hours for inventoty rto be available.
	duration, _ := time.ParseDuration(args.RetryInterval)
	for {
		manifestFile, merr = s3mig.getLatestManifest(ctx, manifestArgs)
		if merr != nil {
			zap.L().Error("Recoverable error during retrieval of latest inventory manifest",
				zap.Error(merr),
			)
			continue
		}
		if manifestFile != nil && manifestFile.Key != nil {
			zap.L().Debug("Found inventory manifest, continuing with batch copy",
				zap.Any("Manifest", manifestFile),
			)
			break
		}
		if ctr > 23 {
			zap.L().Fatal("No inventory manifest found within timeout period, exiting copy process.")
		}
		ctr++
		zap.L().Info("No manifest found, sleeping before retry",
			zap.Int16("retryCount", ctr),
			zap.String("retryInterval", args.RetryInterval),
		)
		time.Sleep(duration)
	}

	//  Setting up non default parameters.
	nonDefaultArgs := &batchJobArgs{
		AccountId:          aws.String(args.AccountID),
		RoleArn:            aws.String(args.RoleArn),
		SourceBucketName:   aws.String(args.SourceBucket),
		TargetBucketName:   aws.String(args.DestinationBucket),
		VersioningDisabled: versioningDisabled,
	}

	// Setting  custom bucket object filters
	filters := userFilters{
		StartDate:  args.StartDt,
		EndDate:    args.EndDt,
		LatestOnly: args.LatestOnly,
		kmsID:      args.KmsID,
	}

	// Build jpb input parameters
	jobParams, err := s3mig.getJobParams(ctx, *manifestFile, nonDefaultArgs, filters)
	if err != nil {
		zap.L().Fatal("Failed to create batch parameters", zap.Error(err))
	}

	// Create S3 batch job(s)
	jobOutput := new(jobResults)
	zap.L().Info("Creating batch job")
	if jobParams.nonVersionJobParam != nil {
		jobOutParam, jobErr := s3mig.s3CtrClient.CreateJob(ctx, jobParams.nonVersionJobParam)
		if jobErr != nil {
			zap.L().Fatal("Failed to create batch job", zap.Error(jobErr))
		}
		jobOutput.nonVersionJobResult, err = s3mig.pollJobResult(ctx, args.AccountID, jobOutParam)
		if err != nil {
			zap.L().Fatal("Failed to get job status",
				zap.String("jobId", *jobOutParam.JobId),
				zap.Error(err),
			)
		}
	}

	if jobParams.versionJobParam != nil {
		// if there is any prior non versioned job, Check its results before proceeding
		if jobParams.nonVersionJobParam != nil {
			if jobOutput.nonVersionJobResult != nil {
				zap.L().Info("Checking non version object job success threshold.")
				jobSuccessThreshold := util.GetJobSuccessThreshold(jobOutput.nonVersionJobResult)
				if jobSuccessThreshold < args.ReqSuccessThreshold {
					zap.L().Fatal("Job Completed, failled to achieve required success threshold",
						zap.Float32("Achieved ", jobSuccessThreshold),
						zap.Float32("Required ", args.ReqSuccessThreshold),
					)
				}
			}

		}
		jobOutParam, jobErr := s3mig.s3CtrClient.CreateJob(ctx, jobParams.versionJobParam)
		if jobErr != nil {
			zap.L().Fatal("Failed to create batch job", zap.Error(jobErr))
		}
		jobOutput.versionJobResult, err = s3mig.pollJobResult(ctx, args.AccountID, jobOutParam)
		if err != nil {
			zap.L().Fatal("Failed to get job status",
				zap.String("jobId", *jobOutParam.JobId),
				zap.Error(err),
			)
		}
	}
	// At last, checking overall job completion success threshold
	jobSuccessThreshold := util.GetJobSuccessThreshold(jobOutput.nonVersionJobResult, jobOutput.versionJobResult)
	if jobSuccessThreshold < args.ReqSuccessThreshold {
		zap.L().Fatal("Job Completed, failed to achieve required success threshold",
			zap.Float32("Achieved ", jobSuccessThreshold),
			zap.Float32("Required ", args.ReqSuccessThreshold),
		)
	}
	zap.L().Info("Job Completed, Achieved required success threshold",
		zap.Float32("Achieved ", jobSuccessThreshold),
		zap.Float32("Required ", args.ReqSuccessThreshold),
	)
	return nil
}

// Polling job progress details and returns job completion details object
func (s3obj *s3migration) pollJobResult(ctx context.Context, accountID string, job *s3control.CreateJobOutput) (*s3control.DescribeJobOutput, error) {
	// Sleep 15 seconds to allow the job to get some kind of update
	zap.L().Info("Sleeping 15 seconds before checking initial job status")
	time.Sleep(15 * time.Second)

	// Poll forever on the state of the batch job
	for {
		jobStatus, jobStatusErr := s3obj.s3CtrClient.DescribeJob(ctx, &s3control.DescribeJobInput{
			AccountId: aws.String(accountID),
			JobId:     job.JobId,
		})
		if jobStatusErr != nil {
			return nil, jobStatusErr
		}
		zap.L().Info("Copy job status",
			zap.String("jobId", *job.JobId),
			zap.Any("status", jobStatus.Job.Status),
			zap.Int64("failed", *jobStatus.Job.ProgressSummary.NumberOfTasksFailed),
			zap.Int64("succeeded", *jobStatus.Job.ProgressSummary.NumberOfTasksSucceeded),
			zap.Int64("total", *jobStatus.Job.ProgressSummary.TotalNumberOfTasks),
		)
		if util.IsTerminal(jobStatus.Job.Status) {
			return jobStatus, nil
		}
		// Unlike manifest polling, we expect S3 Batch operations to complete quickly
		// Therefore we can use a short, standard 60 second poll
		zap.L().Info("Batch job not complete, sleeping 60 seconds before checking status")
		time.Sleep(60 * time.Second)
	}
}

func (s3obj *s3migration) getJobParams(ctx context.Context, manifestFile s3types.Object, jobArgs *batchJobArgs, filters userFilters) (*jobInputParams, error) {

	jobParams := new(jobInputParams)
	createJobInput := func(manifestFile s3types.Object, jobArgs *batchJobArgs, filters userFilters) *s3control.CreateJobInput {
		zap.L().Info("Inventory manifest versioning is disabled, filtering manifest file")
		manifest, err := s3obj.filterManifestCsv(ctx, jobArgs, manifestFile, filters)
		if err != nil {
			zap.L().Fatal("Failed to create filtered manifest file", zap.Error(err))
		}

		manifestObjectArn := util.GetArn(fmt.Sprintf("%s/%s", *jobArgs.SourceBucketName, *manifest.Key))
		zap.L().Debug("Manifest object ARN", zap.String("ARN", *manifestObjectArn))
		jobArgs.ManifestETag = manifest.ETag
		jobArgs.ManifestArn = manifestObjectArn

		jobInputs := NewCreateJobInput(jobArgs)
		// If the target bucket ACL setting is "BucketOwnerEnforced", then
		// use a canned ACL to avoid issues of invalid source object ACLs
		enforced, err := s3obj.isOwnershipEnforced(ctx, *jobArgs.TargetBucketName)
		if err != nil {
			zap.L().Warn("Failed to get destination bucket ownership setting", zap.Error(err))
		}
		if err == nil && enforced {
			zap.L().Info("Destination bucket ownership setting is enforced, using canned bucket owner full control ACL")
			jobInputs.Operation.S3PutObjectCopy.CannedAccessControlList = s3controltypes.S3CannedAccessControlListBucketOwnerFullControl
		}

		return jobInputs
	}

	// For non version bucket create non version job paramters
	if jobArgs.VersioningDisabled {
		jobParams.nonVersionJobParam = createJobInput(manifestFile, jobArgs, filters)
		return jobParams, nil
	}
	// Incase user has requested for latest objects only from versioned bucket
	if filters.LatestOnly == "Yes" {
		jobParams.versionJobParam = createJobInput(manifestFile, jobArgs, filters)
		return jobParams, nil
	}

	// In case no version filter is provided we need to create two jobs one for latest versioned objects
	// another is for non latest versioned objects. we will be copying non latest version objects first and then latest version,
	// by doing this, we will be avoiding any overwriting of older version object over newer version
	filters.LatestOnly = "Yes"
	jobParams.versionJobParam = createJobInput(manifestFile, jobArgs, filters)

	filters.LatestOnly = "No"
	jobParams.nonVersionJobParam = createJobInput(manifestFile, jobArgs, filters)

	return jobParams, nil
}
