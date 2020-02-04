package serverlib

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3File struct {
	region *string
	Bucket *string
}

func NewS3(bucket string, region string) *S3File {
	return &S3File{
		Bucket: &bucket,
		region: &region,
	}
}

func (s *S3File) newSess() *session.Session {
	return session.Must(session.NewSession(&aws.Config{
		Region: s.region,
	}))
}

func (s *S3File) Upload(localFile string, remoteFile string) (*string, error) {
	f, err := os.Open(localFile)
	if err != nil {
		fmt.Println("file not found:", err)
		return nil, err
	}
	r, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("read file error:", err)
		return nil, err
	}
	defer f.Close()

	uploader := s3manager.NewUploader(s.newSess(), func(u *s3manager.Uploader) {
		u.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(25 * 1024 * 1024)
	})
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: s.Bucket,
		Key:    &remoteFile,
		Body:   bytes.NewReader(r),
	})
	if err != nil {
		fmt.Println("upload file failed:", err)
		return nil, err
	}
	return &result.Location, nil
}

func (s *S3File) Download(localFile string, remoteFile string) error {
	downloader := s3manager.NewDownloader(s.newSess(), func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024
	})
	file, err := os.Create(localFile)
	if err != nil {
		fmt.Println(err, "Create file failed")
		return err
	}
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: s.Bucket,
			Key:    &remoteFile,
		})
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	return nil
}
