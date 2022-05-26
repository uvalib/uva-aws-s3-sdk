package uva_s3

import "fmt"

// errors

var ErrNotFound = fmt.Errorf("the specified bucket or key does not exist")
var ErrObjectInGlacier = fmt.Errorf("the specified object is archived in glacier")

type UvaS3 interface {
	StatObject(UvaS3Object) (UvaS3Object, error) // get object attributes
	GetToFile(UvaS3Object, string) error         // get contents of an object to a local file
	GetToBuffer(UvaS3Object) ([]byte, error)     // get contents of an object to a supplied buffer
	PutFromFile(UvaS3Object, string) error       // put contents of a file to the named object
	PutFromBuffer(UvaS3Object, []byte) error     // put contents of the supplied buffer to a named object
	RestoreObject(UvaS3Object) error             // initiate the restore of an object from glacier
	DeleteObject(UvaS3Object) error              // delete the named object
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
	Logging bool // do we log
}

// NewUvaS3 factory for our S3 interface
func NewUvaS3(config UvaS3Config) (UvaS3, error) {

	// mock the implementation here if necessary
	s3, err := newUvaS3(config)
	return s3, err
}

// NewUvaS3Object factory for our S3 object (really a helper)
func NewUvaS3Object(bucketName string, keyName string) UvaS3Object {
	// we use -1 as a sentinel value
	return uvaS3ObjectImpl{bucket: bucketName, key: keyName, size: -1}
}

//
// end of file
//
