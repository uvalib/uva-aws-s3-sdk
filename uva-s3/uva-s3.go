package uva_s3

import "fmt"

// errors
var ErrNoGlacierSupport = fmt.Errorf("glacier support not configured")

type UvaS3 interface {
	GetToFile(UvaS3Object, string) error
	GetToBuffer(UvaS3Object) ([]byte, error)
	PutFromFile(UvaS3Object, string) error
	PutFromBuffer(UvaS3Object, []byte) error
	StatObject(UvaS3Object) (UvaS3Object, error)
	RestoreObject(UvaS3Object) error
	DeleteObject(UvaS3Object) error
}

type UvaS3Object interface {
	BucketName() string // the name of the containing bucket
	KeyName() string    // the key
	IsGlacier() bool    // is the object stored in glacier
	IsRestoring() bool  // is the object currently being restored
	IsRestored() bool   // has the object been restored
	Size() int64        // object size

	// more stuff
}

// UvaS3Config our configuration structure
type UvaS3Config struct {
	GlacierSupport bool // do we expect Glacier objects
}

// NewUvaS3 factory for our S3 interface
func NewUvaS3(config UvaS3Config) (UvaS3, error) {

	// mock the implementation here if necessary
	s3, err := newUvaS3(config)
	return s3, err
}

// NewUvaS3Object factory for our S3 object (really a helper)
func NewUvaS3Object(bucketName string, keyName string) UvaS3Object {
	return uvaS3ObjectImpl{bucket: bucketName, key: keyName}
}

//
// end of file
//
