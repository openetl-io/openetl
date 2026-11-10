package objectstore

import (
	"sync"
	"testing"
)

// AskRegionAt sends S3Client's and Resolve's region requests to ep, with no
// regions known, until the test ends.
func AskRegionAt(t testing.TB, ep string) {
	regionEndpoint = ep
	bucketRegions.Clear()
	t.Cleanup(func() {
		regionEndpoint = ""
		bucketRegions.Clear()
	})
}

// ReloadAWSConfig makes the next client load the AWS config again, as the
// test's environment has it.
func ReloadAWSConfig(t testing.TB) {
	awsConfig = sync.OnceValues(loadAWSConfig)
	t.Cleanup(func() { awsConfig = sync.OnceValues(loadAWSConfig) })
}
