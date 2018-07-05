package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Hasher interface {
	Sum(bucket, key string) string
}

// Dirty global for this quick hack.
var md5hasher Hasher

type md5sum struct {
	s3Client *s3.S3
	items    map[string]string
	mu       sync.RWMutex
}

// How long are we willing to wait for a S3 file to be donwloaded.
const timeout = 5 * time.Second

func hasher(s3Client *s3.S3, bucket, key, etag string) string {
	if md5hasher == nil {
		md5hasher = &md5sum{
			s3Client: s3Client,
			items:    make(map[string]string),
		}
	}
	return md5hasher.Sum(bucket, key)
}

// Looks up the sum in the cache.
func (c *md5sum) has(lookupKey string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sum, ok := c.items[lookupKey]
	return sum, ok
}

// calc downloads the file from S3 and calculates its checksum.
func (c *md5sum) calc(bucket, key string) string {
	tmpfile, err := ioutil.TempFile("", key)
	if err != nil {
		log.Printf("[ERROR] ioutil.TempFile failed: %s", err)
		return ""
	}
	defer tmpfile.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	downloader := s3manager.NewDownloaderWithClient(c.s3Client)
	_, err = downloader.DownloadWithContext(ctx, tmpfile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("[ERROR] S3 downloader failed: %s", err)
		return ""
	}
	md5hasher := md5.New()
	if _, err := io.Copy(md5hasher, tmpfile); err != nil {
		log.Printf("[ERROR] md5 calculation failed: %s", err)
		return ""
	}
	return hex.EncodeToString(md5hasher.Sum(nil)[:16])
}

func (c *md5sum) Sum(bucket, key string) string {
	var lookupKey = fmt.Sprintf("%s:%s", bucket, key)
	sum, ok := c.has(lookupKey)
	if ok {
		log.Printf("MD5 checksum found in the cache (bucket=%s key=%s sum=%s)", bucket, key, sum)
		return sum
	}
	sum = c.calc(bucket, key)
	log.Printf("MD5 checksum generated (bucket=%s key=%s sum=%s)", bucket, key, sum)
	if sum != "" {
		c.mu.RLock()
		defer c.mu.RUnlock()
		c.items[lookupKey] = sum
	}
	return sum
}
