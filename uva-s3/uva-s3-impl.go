package uva_s3

import (
	"bytes"
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

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false || len(location) == 0 {
		return ErrBadParameter
	}

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
			case s3.ErrCodeInvalidObjectState:
				//	log.Printf("ERROR: inappropriate storage class for get (%s)", aerr.Error())
				return ErrObjectInGlacier
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
	if obj.Size() != -1 && obj.Size() != fileSize {

		// remove the file
		_ = os.Remove(location)
		return fmt.Errorf("download failure. expected %d bytes, received %d bytes", obj.Size(), fileSize)
	}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("get of %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds()))
	return nil
}

func (impl *uvaS3Impl) GetToBuffer(obj UvaS3Object) ([]byte, error) {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false {
		return nil, ErrBadParameter
	}

	expectedSize := obj.Size()
	// if we do not yet know the filesize
	if expectedSize == -1 {
		s, err := impl.StatObject(obj)
		if err != nil {
			return nil, err
		}
		expectedSize = s.Size()
	}

	impl.logInfo(fmt.Sprintf("get from s3://%s/%s (%d bytes)", obj.BucketName(), obj.KeyName(), expectedSize))

	start := time.Now()

	backingBuff := make([]byte, 0, expectedSize)
	writeAtBuff := aws.NewWriteAtBuffer(backingBuff)
	downloadSize, err := impl.downloader.Download(writeAtBuff,
		&s3.GetObjectInput{
			Bucket: aws.String(obj.BucketName()),
			Key:    aws.String(obj.KeyName()),
		})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				//log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
				return nil, ErrNotFound
			case s3.ErrCodeNoSuchKey:
				//log.Printf("ERROR: key does not exist (%s)", aerr.Error())
				return nil, ErrNotFound
			case s3.ErrCodeInvalidObjectState:
				//	log.Printf("ERROR: inappropriate storage class for get (%s)", aerr.Error())
				return nil, ErrObjectInGlacier
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
	}

	// we validate the expected file size against the actually downloaded size
	if expectedSize != downloadSize {
		impl.logWarn(fmt.Sprintf("get s3://%s/%s... expected %d bytes, received %d bytes", obj.BucketName(), obj.KeyName(), expectedSize, downloadSize))
	}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("get of s3://%s/%s complete in %0.2f seconds", obj.BucketName(), obj.KeyName(), duration.Seconds()))

	return writeAtBuff.Bytes(), nil
}

func (impl *uvaS3Impl) PutFromFile(obj UvaS3Object, location string) error {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false || len(location) == 0 {
		return ErrBadParameter
	}

	source := fmt.Sprintf("s3://%s/%s", obj.BucketName(), obj.KeyName())

	impl.logInfo(fmt.Sprintf("put from %s to %s", location, source))

	// open the file
	file, err := os.Open(location)
	if err != nil {
		// assume the error is file not found... probably reasonable
		return os.ErrNotExist
	}
	defer file.Close()

	// get the filesize
	s, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := s.Size()

	// Upload the file to S3.
	start := time.Now()
	_, err = impl.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(obj.BucketName()),
		Key:    aws.String(obj.KeyName()),
		Body:   file,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				//log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
				return ErrNotFound
			//case s3.ErrCodeNoSuchKey:
			//log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			//	return ErrNotFound
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

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("put %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds()))
	return nil
}

func (impl *uvaS3Impl) PutFromBuffer(obj UvaS3Object, buffer []byte) error {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false || buffer == nil {
		return ErrBadParameter
	}

	bucket := obj.BucketName()
	key := obj.KeyName()
	size := len(buffer)
	impl.logInfo(fmt.Sprintf("put to s3://%s/%s (%d bytes)", bucket, key, size))

	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(buffer),
	}

	start := time.Now()

	// Perform an upload.
	_, err := impl.uploader.Upload(upParams)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				//log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
				return ErrNotFound
			//case s3.ErrCodeNoSuchKey:
			//log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			//	return ErrNotFound
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

	// we validate the expected file size against the actually uploaded size
	//if int64( size ) != uploadSize {
	//	return nil, fmt.Errorf("upload failure. expected %d bytes, actual %d bytes", contentSize, uploadSize)
	//}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("put of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds()))

	return nil
}

func (impl *uvaS3Impl) StatObject(obj UvaS3Object) (UvaS3Object, error) {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false {
		return nil, ErrBadParameter
	}

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
		//	log.Printf("INFO: %s", result)
	}

	o := uvaS3ObjectImpl{bucket: obj.BucketName(), key: obj.KeyName()}

	// get object attributes
	o.isGlacier = result.StorageClass != nil && strings.HasPrefix(*result.StorageClass, "GLACIER")
	o.isRestoring = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"true\"")
	o.isRestored = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"false\"")
	o.size = *result.ContentLength
	return o, nil
}

func (impl *uvaS3Impl) RestoreObject(obj UvaS3Object, tier int, days int64) error {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false {
		return ErrBadParameter
	}

	tierStr := ""
	switch tier {
	case RESTORE_EXPEDITED:
		tierStr = "Expedited"
	case RESTORE_STANDARD:
		tierStr = "Standard"
	case RESTORE_BULK:
		tierStr = "Bulk"
	default:
		return ErrBadParameter
	}

	impl.logInfo(fmt.Sprintf("restoring: s3://%s/%s tier: %s, %d for days", obj.BucketName(), obj.KeyName(), tierStr, days))

	input := &s3.RestoreObjectInput{
		Bucket: aws.String(obj.BucketName()),
		Key:    aws.String(obj.KeyName()),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(days),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String(tierStr),
			},
		},
	}

	_, err := impl.svc.RestoreObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			//case s3.ErrCodeObjectAlreadyInActiveTierError:
			//	log.Printf("ERROR: already restored (%s)", aerr.Error())
			case s3.ErrCodeInvalidObjectState:
				//log.Printf("ERROR: inappropriate storage class for restore (%s)", aerr.Error())
				return ErrCannotRestore
			case s3.ErrCodeNoSuchBucket:
				//log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
				return ErrNotFound
			case s3.ErrCodeNoSuchKey:
				//log.Printf("ERROR: key does not exist (%s)", aerr.Error())
				return ErrNotFound
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
		//} else {
		//log.Printf("INFO: %s", result)
	}
	return nil
}

func (impl *uvaS3Impl) DeleteObject(obj UvaS3Object) error {

	// validate inbound parameters
	if impl.validateS3Obj(obj) == false {
		return ErrBadParameter
	}

	impl.logInfo(fmt.Sprintf("deleting s3://%s/%s", obj.BucketName(), obj.KeyName()))

	start := time.Now()
	_, err := impl.svc.DeleteObject(
		&s3.DeleteObjectInput{
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
		//} else {
		//log.Printf("INFO: %s", result)
	}

	duration := time.Since(start)
	impl.logInfo(fmt.Sprintf("delete of s3://%s/%s complete in %0.2f seconds", obj.BucketName(), obj.KeyName(), duration.Seconds()))
	return nil
}

//
// helpers
//

func (impl *uvaS3Impl) logInfo(message string) {
	if impl.config.Logging == true {
		log.Printf("INFO: %s", message)
	}
}

func (impl *uvaS3Impl) logWarn(message string) {
	if impl.config.Logging == true {
		log.Printf("WARNING: %s", message)
	}
}

func (impl *uvaS3Impl) logError(message string) {
	if impl.config.Logging == true {
		log.Printf("ERROR: %s", message)
	}
}

func (impl *uvaS3Impl) validateS3Obj(o UvaS3Object) bool {
	if len(o.BucketName()) == 0 || len(o.KeyName()) == 0 {
		return false
	}
	return true
}

//
// uvaS3ObjectImpl implementation methods
//

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
