package util

import (
	"fmt"
	"io"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
	s3controltypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"go.uber.org/zap"
)

// Convert given string to S3 ARN
func GetArn(s string) *string {
	return aws.String(fmt.Sprintf("arn:aws:s3:::%s", s))
}

// An S3 Batch job with a terminal status is one in which there will be no further updates
// to the job status.
func IsTerminal(status s3controltypes.JobStatus) bool {
	return status == s3controltypes.JobStatusComplete ||
		status == s3controltypes.JobStatusFailed ||
		status == s3controltypes.JobStatusCancelled
}

type S3SelectReader struct {
	Stream    *s3.SelectObjectContentEventStream
	remaining []byte // Buffer to store leftover data from previous event
	closed    bool   // Flag indicating whether the reader has been closed
}

func (r *S3SelectReader) Read(b []byte) (n int, err error) {
	// If the reader has been closed, return immediately with an error.
	if r.closed {
		zap.L().Debug("In r.closed condition",
			zap.Bool("closed", r.closed),
			zap.Int("remaining", len(r.remaining)),
		)
		return 0, io.ErrClosedPipe
	}
	var totalBytesRead int
	for {
		// If there is data remaining from the previous event, copy it to the output slice.
		if len(r.remaining) > 0 {
			n := copy(b[totalBytesRead:], r.remaining)
			totalBytesRead += n
			r.remaining = r.remaining[n:]
			if totalBytesRead == len(b) {
				return totalBytesRead, nil
			}
		}

		data, ok := <-r.Stream.Events()
		if !ok {
			zap.L().Debug("EventStream channel closed")
			r.closed = true
			if totalBytesRead > 0 {
				return totalBytesRead, nil
			}
			return 0, io.EOF
		}
		switch v := data.(type) {
		case *s3types.SelectObjectContentEventStreamMemberRecords:
			n := copy(b[totalBytesRead:], v.Value.Payload)
			totalBytesRead += n
			if n < len(v.Value.Payload) {
				r.remaining = v.Value.Payload[n:]
			}
			if totalBytesRead == len(b) {
				return totalBytesRead, nil
			}
		case *s3types.SelectObjectContentEventStreamMemberEnd:
			zap.L().Debug("EventStream ended",
				zap.Int("remaining", len(r.remaining)),
			)
			if totalBytesRead > 0 {
				return totalBytesRead, nil
			}
			return 0, io.EOF
		default:
			// Other events (Progress, Stats, Continuation)
			// don't apply to the io.Reader interface
		}
	}
}

func GetJobSuccessThreshold(jobs ...*s3control.DescribeJobOutput) float32 {
	var (
		totalSuccessThreshold float32
		jobSuccessThreshold   int32
		totalNumberOfTasks    int32
	)
	for _, job := range jobs {
		if job == nil {
			continue
		}
		if *job.Job.ProgressSummary.TotalNumberOfTasks < 1 {
			zap.L().Warn("Job found with zero objects to copy",
				zap.String("Job Id ", *job.Job.JobId),
				zap.String("Job Arn ", *job.Job.JobArn),
			)
			continue
		}
		jobSuccessThreshold = +int32(*job.Job.ProgressSummary.NumberOfTasksSucceeded)
		totalNumberOfTasks = +int32(*job.Job.ProgressSummary.TotalNumberOfTasks)
	}
	if totalNumberOfTasks > 0 {
		totalSuccessThreshold = float32(jobSuccessThreshold) / float32(totalNumberOfTasks)
	}

	return totalSuccessThreshold
}

const (
	LastUpdatedColumn = "LastUpdated"
	IsLatestColumn    = "IsLatest"
	IsLatestYes       = "Yes"
	IsLatestNo        = "No"
)

func GetQueryExpression(fileSchema string, startDt, endDt time.Time, latestOnly string, versioningDisabled bool) (string, error) {
	sql := sq.Select("s._1", "s._2").From("s3object s")

	if versioningDisabled {
		query, _, _ := sql.ToSql()
		return query, nil
	}

	fileSchemaMap, err := parseFileSchema(fileSchema)
	if err != nil {
		return "", err
	}

	getColumnName := func(colName string) (string, error) {
		col, ok := fileSchemaMap[colName]
		if !ok {
			return "", fmt.Errorf("file schema does not contain field '%s', Provided file schema: '%s'", colName, fileSchema)
		}
		return col, nil
	}

	toISO := func(t time.Time) string {
		return t.Format("2006-01-02T15:04:05")
	}

	if len(strings.TrimSpace(latestOnly)) > 0 {
		colName, err := getColumnName(IsLatestColumn)
		if err != nil {
			return "", err
		}
		switch latestOnly {
		case IsLatestYes:
			sql = sql.Where(fmt.Sprintf("%s = 'true'", colName))
		case IsLatestNo:
			sql = sql.Where(fmt.Sprintf("%s = 'false'", colName))
		}
	}

	// Adding date filters
	switch {
	case !startDt.IsZero() && !endDt.IsZero():
		colName, err := getColumnName(LastUpdatedColumn)
		if err != nil {
			zap.L().Warn(err.Error())
		} else {
			sql = sql.Where(fmt.Sprintf("%s BETWEEN '%s' AND '%s'", colName, toISO(startDt), toISO(endDt)))
		}
	case !startDt.IsZero():
		colName, err := getColumnName(LastUpdatedColumn)
		if err != nil {
			zap.L().Warn(err.Error())
		} else {
			sql = sql.Where(fmt.Sprintf("%s < '%s'", colName, toISO(startDt)))
		}
	case !endDt.IsZero():
		colName, err := getColumnName(LastUpdatedColumn)
		if err != nil {
			zap.L().Warn(err.Error())
		} else {
			sql = sql.Where(fmt.Sprintf("%s > '%s'", colName, toISO(endDt)))
		}
	}

	query, _, err := sql.ToSql()
	return query, err
}

func parseFileSchema(fileSchema string) (map[string]string, error) {
	fileSchemaMap := make(map[string]string)
	if strings.LastIndex(fileSchema, ",") < 1 {
		return nil, fmt.Errorf("invalid input file schema: '%s'", fileSchema)
	}
	stringArr := strings.Split(fileSchema, ",")
	for i := 0; i < len(stringArr); i++ {
		fileSchemaMap[strings.TrimSpace(stringArr[i])] = fmt.Sprintf("s._%d", i+1)
	}
	return fileSchemaMap, nil
}

func ParseDateTime(tstr string) (time.Time, error) {
	if len(strings.TrimSpace(tstr)) < 1 {
		return time.Time{}, fmt.Errorf("found invalid input date time string")
	}
	return time.Parse(time.DateTime, tstr)
}

func ZapLogSync() {
	if err := zap.L().Sync(); err != nil {
		fmt.Println(err)
	}

}
