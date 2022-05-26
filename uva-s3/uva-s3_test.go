package uva_s3

import (
	"os"
	"testing"
)

var logging = false
var goodSourceFile = "../Makefile"
var badSourceFile = "hurungl-zit0"
var localSinkFile = "xxx"
var goodBucketName = "uva-dpg3k-scratch"
var badBucketName = "hurungl-zit0"
var goodObjectName = "good-object"
var badObjectName = "bad-object"
var glacierBucketName = "dpg-archive-staging"
var glacierKeyName = "000031989/000031989_0002.tif"

//
// StatObject method invariant tests
//

func TestStatObjectStandardHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// ensure we have a test object available
	uploadTestObject(t, uvas3, goodBucketName, goodObjectName)

	// get the object details
	o := goodS3Object()
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
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

func TestStatObjectGlacierHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := goodGlacierS3Object()
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	if s.BucketName() != glacierBucketName {
		t.Fatalf("Unexpected bucket name. Expected %s, got %s\n", glacierBucketName, s.BucketName())
	}

	if s.KeyName() != glacierKeyName {
		t.Fatalf("Unexpected key name. Expected %s, got %s\n", glacierKeyName, s.KeyName())
	}

	if s.IsGlacier() != true {
		t.Fatalf("Unexpected glacier value. Expected %t, got %t\n", true, s.IsGlacier())
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
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestStatObjectBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badKeyS3Object()
	_, err := uvas3.StatObject(o)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
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
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	err = uvas3.GetToFile(o, localSinkFile)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	// check the results file
	if fileExists(localSinkFile) == false {
		t.Fatalf("Expected results file does not exist\n")
	}

	// verify file size
	sz := fileSize(localSinkFile)
	if sz != s.Size() {
		t.Fatalf("Unexpected size. Expected %d, got %d\n", s.Size(), sz)
	}
}

func TestGetToFileBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badBucketS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestGetToFileBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badKeyS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestGetToFileGlacierObject(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object
	o := goodGlacierS3Object()
	err := uvas3.GetToFile(o, localSinkFile)
	expected := ErrObjectInGlacier
	if err != expected {
		errorEvaluate(t, expected, err)
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
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	b, err := uvas3.GetToBuffer(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	// check the results
	if int64(len(b)) != s.Size() {
		t.Fatalf("Unexpected size. Expected %d, got %d\n", s.Size(), len(b))
	}
}

func TestGetToBufferGlacierObject(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object
	o := goodGlacierS3Object()
	_, err := uvas3.GetToBuffer(o)
	expected := ErrObjectInGlacier
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestGetToBufferBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badBucketS3Object()
	_, err := uvas3.GetToBuffer(o)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestGetToBufferBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badKeyS3Object()
	_, err := uvas3.GetToBuffer(o)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

//
// PutFromFile method invariant tests
//

func TestPutFromFileHappyDay(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// delete the object
	o := goodS3Object()
	err := uvas3.DeleteObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	err = uvas3.PutFromFile(o, goodSourceFile)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	// get uploaded object details
	s, err := uvas3.StatObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	// verify file size
	sz := fileSize(goodSourceFile)
	if sz != s.Size() {
		t.Fatalf("Unexpected size. Expected %d, got %d\n", s.Size(), sz)
	}
}

func TestPutFromFileBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := badBucketS3Object()
	err := uvas3.PutFromFile(o, goodSourceFile)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestPutFromFileBadFileName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object details
	o := goodS3Object()
	err := uvas3.PutFromFile(o, badSourceFile)
	expected := os.ErrNotExist
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

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

	// delete the object
	o := goodS3Object()
	err := uvas3.DeleteObject(o)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	// verify object does not exist
	if objectExists(t, uvas3, o) != false {
		t.Fatalf("Object was not deleted successfully\n")
	}
}

func TestDeleteObjectBadBucketName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object
	o := badBucketS3Object()
	err := uvas3.DeleteObject(o)
	expected := ErrNotFound
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

func TestDeleteObjectBadKeyName(t *testing.T) {

	// test setup
	uvas3 := testSetup(t)

	// get the object
	o := badKeyS3Object()
	err := uvas3.DeleteObject(o)
	//expected := ErrNotFound
	var expected error = nil // the AWS S3 API will return success when deleting a non-existent object
	if err != expected {
		errorEvaluate(t, expected, err)
	}
}

//
// helper methods
//

func uploadTestObject(t *testing.T, uvas3 UvaS3, bucket string, key string) {

	o := NewUvaS3Object(bucket, key)
	err := uvas3.PutFromFile(o, goodSourceFile)
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}
}

func objectExists(t *testing.T, uvas3 UvaS3, object UvaS3Object) bool {
	_, err := uvas3.StatObject(object)
	switch err {
	case nil:
		return true
	case ErrNotFound:
		return false
	}
	t.Fatalf("%s\n", err.Error())
	return true // silly compiler
}

func testSetup(t *testing.T) UvaS3 {
	uvas3, err := NewUvaS3(UvaS3Config{Logging: logging})
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return uvas3
}

func errorEvaluate(t *testing.T, expected error, actual error) {
	if expected != nil {
		if actual != nil {
			t.Fatalf("Unexpected error. Expected (%s), got (%s)\n", expected.Error(), actual.Error())
		} else {
			t.Fatalf("Unexpected error. Expected (%s), got (nill)\n", expected.Error())
		}
	} else {
		// we know actual is not nill
		t.Fatalf("Unexpected error. Expected (nill), got (%s)\n", actual.Error())
	}
}

func goodS3Object() UvaS3Object {
	return NewUvaS3Object(goodBucketName, goodObjectName)
}

func goodGlacierS3Object() UvaS3Object {
	return NewUvaS3Object(glacierBucketName, glacierKeyName)
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

func fileSize(filename string) int64 {
	fi, err := os.Stat(filename)
	if err != nil {
		// cos we have already checked that it exists
		return 0
	}
	return fi.Size()
}

func deleteFile(filename string) {
	os.Remove(filename)
}

//
// end of file
//
