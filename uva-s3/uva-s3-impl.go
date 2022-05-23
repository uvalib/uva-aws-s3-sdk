package uva_s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
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
	placeholder bool  // is this a placeholder object or is it fully populated from stat
}

// factory for our S3 interface
func newUvaS3(config UvaS3Config) (UvaS3, error) {

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	var impl uvaS3Impl
	impl.uploader = s3manager.NewUploader(sess)
	impl.downloader = s3manager.NewDownloader(sess)
	impl.svc = s3.New(sess)

	return &impl, nil
}

func (impl *uvaS3Impl) GetToFile(obj UvaS3Object, location string) error {

	//	source := fmt.Sprintf("s3:/%s/%s", obj.BucketName(), obj.KeyName())
	//	log.Printf("INFO: get %s to %s", source, location)
	//
	//	start := time.Now()
	//	fileSize, err := impl.downloader.Download(file,
	//		&s3.GetObjectInput{
	//			Bucket: aws.String(obj.BucketName()),
	//			Key:    aws.String(obj.KeyName()),
	//		})
	//
	//	if err != nil {
	//		return err
	//	}
	//
	//	// I think there are times when the download runs out of space but it is not reported as an error so
	//	// we validate the expected file size against the actually downloaded size
	//	if obj.Size() != 0 && obj.Size() != fileSize {
	//
	//		// remove the file
	//		_ = os.Remove(location)
	//		return fmt.Errorf("download failure. expected %d bytes, received %d bytes", obj.Size(), fileSize)
	//	}
	//
	//	duration := time.Since(start)
	//	log.Printf("INFO: get of %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds())
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
			case "BadRequest":
				log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
			case "NotFound":
				log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			default:
				log.Printf("ERROR: %s (%s)", aerr.Code(), aerr.Error())
			}
			return nil, aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return nil, err
		}
	} else {
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

	log.Printf("INFO: deleting s3://%s/%s", obj.BucketName(), obj.KeyName())

	start := time.Now()
	_, err := impl.svc.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(obj.BucketName()),
			Key:    aws.String(obj.KeyName()),
		})
	if err != nil {
		log.Printf("ERROR: deleting s3://%s/%s (%s)", obj.BucketName(), obj.KeyName(), err.Error())
		return err
	}

	duration := time.Since(start)
	log.Printf("INFO: delete of s3://%s/%s complete in %0.2f seconds", obj.BucketName(), obj.KeyName(), duration.Seconds())
	return nil
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
