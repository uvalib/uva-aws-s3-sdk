package uva_s3

import "testing"

var goodBucketName = "uva-dpg3k-scratch"
var badBucketName = "bla-bla-bla"
var goodObjectName = "good-object"
var badObjectName = "bad-object"

//
// StatObject method invariant tests
//

func TestStatObjectHappyDay(t *testing.T) {

	uvas3, err := NewUvaS3(UvaS3Config{})
	if err != nil {
		t.Fatalf("%t\n", err)
	}

	// ensure we have a test object available
	makeTestObject(t, uvas3, goodBucketName, goodObjectName)

	// get the object details
	o := NewUvaS3Object(goodBucketName, goodObjectName)
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

//
// helper methods
//

func makeTestObject(t *testing.T, uvas3 UvaS3, bucket string, key string) {

}

//
// end of file
//
