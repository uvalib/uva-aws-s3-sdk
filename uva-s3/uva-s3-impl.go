package uva_s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"os"
	"strings"
	"time"
)

// this is our s3 interface implementation
type uvaS3Impl struct {
	config     UvaS3Config
	svc        *s3.S3
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

// this is our s3 object implementation
type uvaS3ObjectImpl struct {
	bucket      string
	key         string
	isGlacier   bool  // is the object stored in glacier
	isRestoring bool  // is the object currently being restored
	isRestored  bool  // has the object been restored
	size        int64 // object size
}

// factory for our S3 interface
func newUvaS3(config UvaS3Config) (UvaS3, error) {

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	var impl uvaS3Impl
	impl.config = config
	impl.uploader = s3manager.NewUploader(sess)
	impl.downloader = s3manager.NewDownloader(sess)
	impl.svc = s3.New(sess)

	return &impl, nil
}

func (impl *uvaS3Impl) GetToFile(obj UvaS3Object, location string) error {

	source := fmt.Sprintf("s3://%s/%s", obj.BucketName(), obj.KeyName())

	impl.logInfo(fmt.Sprintf("get %s to %s", source, location))

	file, err := os.OpenFile(location, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	start := time.Now()
	fileSize, err := impl.downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(obj.BucketName()),
			Key:    aws.String(obj.KeyName()),
		})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				//log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
				return ErrNotFound
			case s3.ErrCodeNoSuchKey:
				//log.Printf("ERROR: key does not exist (%s)", aerr.Error())
				return ErrNotFound
			//case s3.ErrCodeInvalidObjectState:
			//	log.Printf("ERROR: inappropriate storage class for get (%s)", aerr.Error())
			default:
				impl.logError(fmt.Sprintf("%s (%s)", aerr.Code(), aerr.Error()))
			}
			return aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			impl.logError(fmt.Sprintf("%s", err.Error()))
			return err
		}
	}

	//	// I think there are times when the download runs out of space but it is not reported as an error so
	//	// we validate the expected file size against the actually downloaded size
	if obj.Size() != 0 && obj.Size() != fileSize {

		// remove the file
		_ = os.Remove(location)
		return fmt.Errorf("download failure. expected %d bytes, received %d bytes", obj.Size(), fileSize)
	}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("get of %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds()))
	return nil
}

func (impl *uvaS3Impl) GetToBuffer(obj UvaS3Object) ([]byte, error) {
	return nil, nil
}

func (impl *uvaS3Impl) PutFromFile(obj UvaS3Object, location string) error {
	return nil
}

func (impl *uvaS3Impl) PutFromBuffer(obj UvaS3Object, buffer []byte) error {
	return nil
}

func (impl *uvaS3Impl) StatObject(obj UvaS3Object) (UvaS3Object, error) {

	input := &s3.HeadObjectInput{
		Bucket: aws.String(obj.BucketName()),
		Key:    aws.String(obj.KeyName()),
	}

	result, err := impl.svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				//log.Printf("ERROR: bucket/key does not exist (%s)", aerr.Error())
				return nil, ErrNotFound
			default:
				impl.logError(fmt.Sprintf("%s (%s)", aerr.Code(), aerr.Error()))
			}
			return nil, aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			impl.logError(fmt.Sprintf("%s", err.Error()))
			return nil, err
		}
		//} else {
		//log.Printf("INFO: %s", result)
	}

	o := uvaS3ObjectImpl{bucket: obj.BucketName(), key: obj.KeyName()}

	// get object attributes
	o.isGlacier = result.StorageClass != nil && strings.HasPrefix(*result.StorageClass, "GLACIER")
	o.isRestoring = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"true\"")
	o.isRestored = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"false\"")
	o.size = *result.ContentLength
	return o, nil
}

func (impl *uvaS3Impl) RestoreObject(obj UvaS3Object) error {

	if impl.config.GlacierSupport == false {
		// may not need???
	}

	return nil
}

func (impl *uvaS3Impl) DeleteObject(obj UvaS3Object) error {

	impl.logInfo(fmt.Sprintf("deleting s3://%s/%s", obj.BucketName(), obj.KeyName()))

	start := time.Now()
	_, err := impl.svc.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(obj.BucketName()),
			Key:    aws.String(obj.KeyName()),
		})
	if err != nil {
		impl.logError(fmt.Sprintf("deleting s3://%s/%s (%s)", obj.BucketName(), obj.KeyName(), err.Error()))
		return err
	}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("delete of s3://%s/%s complete in %0.2f seconds", obj.BucketName(), obj.KeyName(), duration.Seconds()))
	return nil
}

func (impl *uvaS3Impl) logInfo(message string) {
	if impl.config.Logging == true {
		log.Printf("INFO: %s", message)
	}
}

func (impl *uvaS3Impl) logError(message string) {
	if impl.config.Logging == true {
		log.Printf("ERROR: %s", message)
	}
}

func (impl uvaS3ObjectImpl) BucketName() string {
	return impl.bucket
}

func (impl uvaS3ObjectImpl) KeyName() string {
	return impl.key
}

func (impl uvaS3ObjectImpl) IsGlacier() bool {
	return impl.isGlacier
}

func (impl uvaS3ObjectImpl) IsRestoring() bool {
	return impl.isRestoring
}

func (impl uvaS3ObjectImpl) IsRestored() bool {
	return impl.isRestored
}

func (impl uvaS3ObjectImpl) Size() int64 {
	return impl.size
}

//
// end of file
//
