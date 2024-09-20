package aws

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Config struct {
	region    string
	accessKey string
	secretKey string
	bucket    string
}

// S3Client wraps the AWS S3 client
type S3Client struct {
	s3     *s3.S3
	bucket string
}

// NewS3Client initializes a new S3 client
func NewS3Client(config S3Config) *S3Client {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(config.region),
		Credentials: credentials.NewStaticCredentials(config.accessKey, config.secretKey, ""),
	}))
	return &S3Client{
		s3:     s3.New(sess),
		bucket: config.bucket,
	}
}

// UploadFile uploads a file to the specified S3 bucket
func (c *S3Client) UploadFile(key string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info %q, %v", filePath, err)
	}

	buffer := make([]byte, fileInfo.Size())
	_, err = file.Read(buffer)
	if err != nil {
		return fmt.Errorf("failed to read file %q, %v", filePath, err)
	}

	_, err = c.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buffer),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to s3 %q, %v", filePath, err)
	}

	return nil
}

// DownloadFile downloads a file from the specified S3 bucket
func (c *S3Client) DownloadFile(key string, filePath string) error {
	result, err := c.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from s3 %q, %v", key, err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return fmt.Errorf("failed to read s3 object body %q, %v", key, err)
	}

	err = os.WriteFile(filePath, body, 0644)

	if err != nil {
		return fmt.Errorf("failed to write file %q, %v", filePath, err)
	}

	return nil
}

// ListObjects lists objects in the specified S3 bucket
func (c *S3Client) ListObjects() ([]string, error) {
	result, err := c.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in bucket %q, %v", c.bucket, err)
	}

	var objects []string
	for _, item := range result.Contents {
		objects = append(objects, *item.Key)
	}

	return objects, nil
}

// DeleteObject deletes an object from the specified S3 bucket
func (c *S3Client) DeleteObject(key string) error {
	_, err := c.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object %q, %v", key, err)
	}

	return nil
}
