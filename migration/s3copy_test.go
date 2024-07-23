package migration

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

type mock struct {
	listObjectsV2Output *s3.ListObjectsV2Output
}

func (m *mock) GetBucketVersioning(ctx context.Context, params *s3.GetBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{Status: "Disabled"}, nil
}

func (m *mock) GetBucketInventoryConfiguration(ctx context.Context, params *s3.GetBucketInventoryConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketInventoryConfigurationOutput, error) {
	return nil, nil
}

func (m *mock) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	stringReader := strings.NewReader(`{
		"sourceBucket" : "testsurcebucket",
		"destinationBucket" : "arn:aws:s3:::inventorybucket-test1",
		"version" : "2016-11-30",
		"creationTimestamp" : "1713142800000",
		"fileFormat" : "CSV",
		"fileSchema" : "Bucket, Key, VersionId, IsLatest, IsDeleteMarker",
		"files" : [ {
		  "key" : "testsurcebucket/put-bucket-inventory-configuration/data/573a77fc-0397-4005-96d6-13a758a7635a.csv.gz",
		  "size" : 169,
		  "MD5checksum" : "1100734f544982bc66291b44c40743c1"
		} ]
	  }
	  `)
	stringReadCloser := io.NopCloser(stringReader)
	return &s3.GetObjectOutput{Body: stringReadCloser}, nil
}

func (m *mock) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.listObjectsV2Output, nil
}

func (m *mock) PutBucketInventoryConfiguration(ctx context.Context, params *s3.PutBucketInventoryConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketInventoryConfigurationOutput, error) {
	return &s3.PutBucketInventoryConfigurationOutput{}, nil
}
func (m *mock) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (m *mock) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mock) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil

}
func (m *mock) SelectObjectContent(c context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error) {
	return &s3.SelectObjectContentOutput{}, nil
}

func (m *mock) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{}, nil
}

func (m *mock) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}

func (m *mock) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}

func (m *mock) GetBucketOwnershipControls(ctx context.Context, params *s3.GetBucketOwnershipControlsInput, optFns ...func(*s3.Options)) (*s3.GetBucketOwnershipControlsOutput, error) {
	return nil, nil
}

var s3mig *s3migration

func TestIsVersioningDisabled(t *testing.T) {
	s3mig = &s3migration{s3Client: new(mock)}
	_, er := s3mig.isVersioningDisabled(context.TODO(), "testbucket")
	if er != nil {
		t.Error("failed to validate bucker versioning")
	}
}

func TestEnsureS3InventoryConfig(t *testing.T) {
	s3mig = &s3migration{s3Client: new(mock)}
	v, er := s3mig.ensureS3InventoryConfig(context.TODO(), "testbucket", "testconfig", false)
	if er != nil {
		t.Errorf("failed %v", er)
	}
	if v.BucketName != "testbucket" {
		t.Error("failed to create inventory config")
	}
}

func TestBuildCopyJobArgs(t *testing.T) {
	s3mig = &s3migration{s3Client: new(mock)}
	out := NewCreateJobInput(&batchJobArgs{
		AccountId:          aws.String("1112223334"),
		RoleArn:            aws.String("arn:aws:iam::1112223334:role/somedummyrole"),
		TargetBucketName:   aws.String("dummybucket"),
		ManifestETag:       aws.String("1123123123134wer3242dasdas"),
		ManifestArn:        aws.String("arn:aws:s3:::mytestbucket/manifest.json"),
		VersioningDisabled: true,
	})
	if *out.AccountId != "1112223334" {
		t.Error("failed to create inventory config")
	}
}

func TestGetLatestManifest(t *testing.T) {
	s3mig = &s3migration{s3Client: &mock{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			CommonPrefixes: []s3types.CommonPrefix{},
			Contents: []s3types.Object{{ETag: aws.String("/testetag/"),
				Key: aws.String("/inventorybucket/manifest.json"), LastModified: aws.Time(time.Now().Add(-1))}},
		},
	}}
	out, er := s3mig.getLatestManifest(context.TODO(), &inventoryManifestFinderArgs{
		BucketName: "testsurcebucket",
		Prefix:     "testsurcebucket/bulk-copy-inventory/",
		DateWindow: -1,
	})
	if er != nil {
		t.Errorf("failed %v", er)
	}
	if !strings.HasSuffix(*out.Key, "manifest.json") {
		t.Error("failed to get latest manifest")
	}
}

func TestGetLatestManifest_2(t *testing.T) {
	testCases := []struct {
		name           string
		finderArgs     *inventoryManifestFinderArgs
		listObjectsOut *s3.ListObjectsV2Output
		expectedErr    error
		expectedObj    *s3types.Object
	}{
		{
			name: "NoManifestFound",
			finderArgs: &inventoryManifestFinderArgs{
				BucketName: "test-bucket",
				Prefix:     "prefix/",
				DateWindow: 24,
			},
			listObjectsOut: &s3.ListObjectsV2Output{
				Contents: []s3types.Object{},
			},
			expectedErr: nil,
			expectedObj: nil,
		},
		// {
		// 	name: "ManifestFound",
		// 	finderArgs: &inventoryManifestFinderArgs{
		// 		BucketName: "test-bucket",
		// 		Prefix:     "prefix/",
		// 		DateWindow: 24,
		// 	},
		// 	listObjectsOut: &s3.ListObjectsV2Output{
		// 		Contents: []s3types.Object{
		// 			{
		// 				Key:          aws.String("prefix/2023-04-01/manifest.json"),
		// 				LastModified: aws.Time(time.Now()),
		// 			},
		// 		},
		// 	},
		// 	expectedErr: nil,
		// 	expectedObj: &s3types.Object{
		// 		Key:          aws.String("prefix/2023-04-01/manifest.json"),
		// 		LastModified: aws.Time(time.Now()),
		// 	},
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockS3Client := &mock{
				listObjectsV2Output: tc.listObjectsOut,
			}

			s3mig = &s3migration{s3Client: mockS3Client}

			obj, err := s3mig.getLatestManifest(context.Background(), tc.finderArgs)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedObj, obj)
		})
	}
}
