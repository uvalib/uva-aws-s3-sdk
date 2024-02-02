package uva_s3

import (
	"fmt"
	"time"
)

// errors

var ErrBadParameter = fmt.Errorf("bad parameter")
var ErrNotFound = fmt.Errorf("the specified bucket or key does not exist")
var ErrObjectInGlacier = fmt.Errorf("the specified object is archived in glacier")
var ErrCannotRestore = fmt.Errorf("the specified object cannot be restored as it is NOT archived in glacier")

type UvaS3 interface {
	StatObject(UvaS3Object) (UvaS3Object, error) // get object attributes
	GetToFile(UvaS3Object, string) error         // get contents of an object to a local file
	GetToBuffer(UvaS3Object) ([]byte, error)     // get contents of an object to a supplied buffer
	PutFromFile(UvaS3Object, string) error       // put contents of a file to the named object
	PutFromBuffer(UvaS3Object, []byte) error     // put contents of the supplied buffer to a named object
	RestoreObject(UvaS3Object, int, int64) error // initiate the restore of an object from glacier
	DeleteObject(UvaS3Object) error              // delete the named object
}

type UvaS3Object interface {
	BucketName() string      // the name of the containing bucket
	KeyName() string         // the key
	IsGlacier() bool         // is the object stored in glacier
	IsRestoring() bool       // is the object currently being restored
	IsRestored() bool        // has the object been restored
	Size() int64             // object size
	LastModified() time.Time // last modified time

	// more stuff
}

// used for the type of restore
const (
	RESTORE_EXPEDITED = iota // Expedited retrievals allow you to quickly access your data, typically made available within 1–5 minutes
	RESTORE_STANDARD         // Standard retrievals allow you to access any of your archived objects within several hours
	RESTORE_BULK             // Bulk retrievals are the lowest-cost retrieval option and typically finish within 5–12 hours
	RESTORE_UNDEFINED
)

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
	return newUvaS3Object(bucketName, keyName)
}

//
// end of file
//
