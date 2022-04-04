package lsmt

import (
	"context"
	"fmt"
	"log"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type remotetable struct {
	bucketName string

	client *minio.Client
}

func NewRemoteTable(bucketName string) (*remotetable, error) {
	endpoint := "192.168.0.251:9000"
	accessKeyID := "wangrushen"
	secretAccessKey := "wangrushen"
	useSSL := false

	// Initialize minio client object.
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := client.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			return nil, err
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	return &remotetable{bucketName, client}, nil
}

func (r *remotetable) Close() error {
	return nil
}

// func (r *remotetable) getRange(start, end int64) {
// 	objectCh := r.client.ListObjects(context.Background(), r.bucketName, minio.ListObjectsOptions{})
// 	for object := range objectCh {
// 		if object.Err != nil {
// 			fmt.Println(object.Err)
// 			return
// 		}
// 		fmt.Println(object)
// 	}
// }

func (r *remotetable) uploadFile(p string) error {
	fmt.Println("uploading")
	objectName := path.Base(p)
	filePath := p

	info, err := r.client.FPutObject(context.Background(), r.bucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
	return nil
}
