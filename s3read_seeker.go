package s3ReadSeeker

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Object struct {
	client     *s3.Client
	bucketName string
	key        string
	size       int64
	offset     int64
}

func (o *Object) ReadAt(p []byte, off int64) (n int, err error) {
	byteRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	input := &s3.GetObjectInput{
		Bucket: aws.String(o.bucketName),
		Key:    aws.String(o.key),
		Range:  aws.String(byteRange),
	}
	result, err := o.client.GetObject(context.TODO(), input)
	if err != nil {
		return 0, err
	}
	defer result.Body.Close()
	return io.ReadFull(result.Body, p)

}

type S3ReadSeeker struct {
	client        *s3.Client
	bucketName    string
	objectMembers []*Object
	globalOffset  int64
	mu            sync.Mutex
}

func NewS3ReadSeeker(client *s3.Client, bucketName string, keyGroup []string) (rs *S3ReadSeeker, err error) {
	rs = &S3ReadSeeker{
		client:        client,
		bucketName:    bucketName,
		objectMembers: make([]*Object, len(keyGroup)),
		globalOffset:  0,
	}
	for n, key := range keyGroup {
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}
		result, err := client.HeadObject(context.TODO(), headInput)
		if err != nil {
			return nil, err
		}
		rs.objectMembers[n] = &Object{
			client:     client,
			bucketName: bucketName,
			key:        key,
			size:       *result.ContentLength,
			offset:     0,
		}
	}
	return rs, nil
}

func (s *S3ReadSeeker) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err = s.ReadAt(p, s.globalOffset)
	if err != nil {
		return n, err
	}
	s.globalOffset += int64(n)
	return n, err
}

func (s *S3ReadSeeker) ReadAt(p []byte, off int64) (n int, err error) {
	var pOff int64
	for _, obj := range s.objectMembers {
		if off >= obj.size {
			// offset exceedes the object size
			// skip it and rewind the offset
			off = off - obj.size
			continue
		}
		// end is s3 range end, it's closed interval
		end := off + int64(len(p[pOff:])) - 1
		// if end exceeds the object size, we need to read from the end of the object
		if end+1 > obj.size {
			newPOff := pOff + (obj.size - off)
			m, err := obj.ReadAt(p[pOff:newPOff], off)
			if err != nil {
				return n, err
			}
			pOff = newPOff
			n += m
			off = 0
			continue
		}
		// read last part
		m, err := obj.ReadAt(p[pOff:], off)
		if err != nil {
			return n, err
		}
		n += m
		return n, nil
	}
	return 0, io.EOF
}

func (s *S3ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = s.globalOffset + offset
	case io.SeekEnd:
		for _, obj := range s.objectMembers {
			newOffset += obj.size
		}
		newOffset += offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("invalid offset: %d", newOffset)
	}
	s.globalOffset = newOffset
	return s.globalOffset, nil
}
