package migration

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
)

type inventoryManifestFinderArgs struct {
	BucketName string
	Prefix     string
	DateWindow int
}

type MigrationArgs struct {
	SourceRegion        string
	AccountID           string
	SourceBucket        string
	RoleArn             string
	DestinationBucket   string
	RetryInterval       string
	ConfigName          string
	StartDt             time.Time
	EndDt               time.Time
	LatestOnly          string
	KmsID               string
	ReqSuccessThreshold float32
	Region              string
}
type batchJobArgs struct {
	AccountId          *string // Account hosting the batch job
	RoleArn            *string // IAM role used by S3 Batch operation
	SourceBucketName   *string // S3 bucket that content is being copied from
	TargetBucketName   *string // S3 bucket that content is being copied to
	ManifestArn        *string // ARN pointing to manifest.json created by inventory process
	ManifestETag       *string // ETag of manifest.json created by inventory process
	VersioningDisabled bool    // True if versioning is disable on source bucket
}

// Expected format of S3 inventory manifest.json
type manifestJson struct {
	Files []struct {
		Key string `json:"key"`
	} `json:"files"`
	FileSchema string `json:"fileSchema"`
}

type userFilters struct {
	StartDate  time.Time
	EndDate    time.Time
	LatestOnly string
	kmsID      string
}

type jobInputParams struct {
	versionJobParam    *s3control.CreateJobInput
	nonVersionJobParam *s3control.CreateJobInput
}

type jobResults struct {
	versionJobResult    *s3control.DescribeJobOutput
	nonVersionJobResult *s3control.DescribeJobOutput
}

// https://pkg.go.dev/slices#SortFunc
// slices.SortFunc is easier than sort interface
func objectDateDescending(a, b s3types.Object) int {
	return b.LastModified.Compare(*a.LastModified)
}

type s3API interface {
	PutBucketInventoryConfiguration(ctx context.Context, params *s3.PutBucketInventoryConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketInventoryConfigurationOutput, error)
	GetBucketInventoryConfiguration(ctx context.Context, params *s3.GetBucketInventoryConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketInventoryConfigurationOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetBucketVersioning(ctx context.Context, params *s3.GetBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	SelectObjectContent(c context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	GetBucketOwnershipControls(ctx context.Context, params *s3.GetBucketOwnershipControlsInput, optFns ...func(*s3.Options)) (*s3.GetBucketOwnershipControlsOutput, error)
}

type s3ControlAPI interface {
	CreateJob(ctx context.Context, params *s3control.CreateJobInput, optFns ...func(*s3control.Options)) (*s3control.CreateJobOutput, error)
	DescribeJob(ctx context.Context, params *s3control.DescribeJobInput, optFns ...func(*s3control.Options)) (*s3control.DescribeJobOutput, error)
}
