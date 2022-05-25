package uva_s3

import (
	"os"
	"testing"
)

var localSourceFile = "Makefile"
var localSinkFile = "xxx"
var goodBucketName = "uva-dpg3k-scratch"
var badBucketName = "hurungl-zit0"
var goodObjectName = "good-object"
var badObjectName = "bad-object"

//
// StatObject method invariant tests
//

func TestStatObjectHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available
	uploadTestObject(t, uvas3, goodBucketName, goodObjectName)

	// get the object details
	o := goodS3Object()
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%t\n", err)
	}

	if s.BucketName() != goodBucketName {
		t.Fatalf("Unexpected bucket name. Expected %s, got %s\n", goodBucketName, s.BucketName())
	}

	if s.KeyName() != goodObjectName {
		t.Fatalf("Unexpected key name. Expected %s, got %s\n", goodObjectName, s.KeyName())
	}

	if s.IsGlacier() != false {
		t.Fatalf("Unexpected glacier value. Expected %t, got %t\n", false, s.IsGlacier())
	}

	if s.IsRestoring() != false {
		t.Fatalf("Unexpected restoring value. Expected %t, got %t\n", false, s.IsRestoring())
	}

	if s.IsRestored() != false {
		t.Fatalf("Unexpected restored value. Expected %t, got %t\n", false, s.IsRestored())
	}

	if s.Size() == 0 {
		t.Fatalf("Unexpected size value. Expected non-zero, got %d\n", s.Size())
	}
}

func TestStatObjectBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badBucketS3Object()
	_, err := uvas3.StatObject(o)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected (%s), got (%s)\n", ErrNotFound.Error(), err.Error())
	}
}

func TestStatObjectBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badKeyS3Object()
	_, err := uvas3.StatObject(o)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected (%s), got (%s)\n", ErrNotFound.Error(), err.Error())
	}
}

//
// GetToFile method invariant tests
//

func TestGetToFileHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available and delete the local sink file
	uploadTestObject(t, uvas3, goodBucketName, goodObjectName)
	deleteFile(localSinkFile)

	// get the object
	o := goodS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	if err != nil {
		t.Fatalf("%t\n", err)
	}

	// check the results file
	if fileExists(localSinkFile) == false {
		t.Fatalf("Expected results file does not exist\n")
	}
}

func TestGetToFileBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badBucketS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected %s, got %s\n", ErrNotFound.Error(), err.Error())
	}
}

func TestGetToFileBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badKeyS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected %s, got %s\n", ErrNotFound.Error(), err.Error())
	}
}

//
// GetToBuffer method invariant tests
//

func TestGetToBufferHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available and delete the local sink file
	uploadTestObject(t, uvas3, goodBucketName, goodObjectName)

	// get the object
	o := goodS3Object()
	b, err := uvas3.GetToBuffer(o)
	if err != nil {
		t.Fatalf("%t\n", err)
	}

	// check the results
	if len(b) == 0 {
		t.Fatalf("Expected results buffer is empty\n")
	}
}

//
// PutFromFile method invariant tests
//

//
// PutFromBuffer method invariant tests
//

//
// RestoreObject method invariant tests
//

//
// DeleteObject method invariant tests
//

func TestDeleteObjectHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available and delete the local sink file
	uploadTestObject(t, uvas3, goodBucketName, goodObjectName)

	// get the object
	o := goodS3Object()
	err := uvas3.DeleteObject(o)
	if err != nil {
		t.Fatalf("%t\n", err)
	}

	// verify object does not exist
	if objectExists(uvas3, o) != false {
		t.Fatalf("Object was not deleted successfully\n")
	}
}

func TestDeleteObjectBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available and delete the local sink file
	uploadTestObject(t, uvas3, badBucketName, goodObjectName)

	// get the object
	o := goodS3Object()
	err := uvas3.DeleteObject(o)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected %s, got %s\n", ErrNotFound.Error(), err.Error())
	}
}

func TestDeleteObjectBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available and delete the local sink file
	uploadTestObject(t, uvas3, goodBucketName, badObjectName)

	// get the object
	o := goodS3Object()
	err := uvas3.DeleteObject(o)
	if err != ErrNotFound {
		t.Fatalf("Unexpected error. Expected %s, got %s\n", ErrNotFound.Error(), err.Error())
	}
}

//
// helper methods
//

func uploadTestObject(t *testing.T, uvas3 UvaS3, bucket string, key string) {
	// fill me in later
}

func objectExists(uvas3 UvaS3, object UvaS3Object) bool {
	// fill me in later
	return true
}

func testSetup(t *testing.T) UvaS3 {
	uvas3, err := NewUvaS3(UvaS3Config{Logging: true})
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return uvas3
}

func goodS3Object() UvaS3Object {
	return NewUvaS3Object(goodBucketName, goodObjectName)
}

func badBucketS3Object() UvaS3Object {
	return NewUvaS3Object(badBucketName, goodObjectName)
}

func badKeyS3Object() UvaS3Object {
	return NewUvaS3Object(goodBucketName, badObjectName)
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	// assume the error is that the file does not exist
	return false
}

func deleteFile(filename string) {
	os.Remove(filename)
}

//
// end of file
//
