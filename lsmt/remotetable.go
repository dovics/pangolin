package lsmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"

	db "github.com/dovics/pangolin"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type remotetable struct {
	option *RemoteOption
	client *minio.Client

	dt *disktable
}

type RemoteOption struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool

	BucketName string
	WorkDir    string
}

func NewRemoteOption(o *Option) (*RemoteOption, error) {
	if len(o.MinioEndpoint) == 0 || len(o.MinioAccessKeyID) == 0 || len(o.MinioSecretAccessKey) == 0 {
		return nil, errors.New("missing minio config")
	}

	return &RemoteOption{
		Endpoint:        o.MinioEndpoint,
		AccessKeyID:     o.MinioAccessKeyID,
		SecretAccessKey: o.MinioSecretAccessKey,
		UseSSL:          o.MinioUseSSL,
		BucketName:      o.uuid.String(),
		WorkDir:         o.WorkDir,
	}, nil
}

func NewRemoteTable(option *RemoteOption, dt *disktable) (*remotetable, error) {
	// Initialize minio client object.
	client, err := minio.New(option.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(option.AccessKeyID, option.AccessKeyID, ""),
		Secure: option.UseSSL,
	})

	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	err = client.MakeBucket(ctx, option.BucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := client.BucketExists(ctx, option.BucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", option.BucketName)
		} else {
			fmt.Println("(" + option.BucketName + ")")
			return nil, err
		}
	} else {
		log.Printf("Successfully created %s\n", option.BucketName)
	}

	return &remotetable{
		option: option,
		client: client,
		dt:     dt,
	}, nil
}

func (r *remotetable) Close() error {
	return nil
}

func (r *remotetable) upload(p string) error {
	objectName := path.Base(p)
	filePath := p

	info, err := r.client.FPutObject(context.Background(), r.option.BucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
	return nil
}

func (r *remotetable) download(name string) error {
	if err := r.client.FGetObject(
		context.Background(),
		r.option.BucketName,
		name,
		path.Join(r.option.WorkDir, name),
		minio.GetObjectOptions{},
	); err != nil {
		return err
	}

	return nil
}

func (r *remotetable) getRange(start, end int64, filter *db.QueryFilter) ([]db.KV, error) {
	result := []db.KV{}
	objectCh := r.client.ListObjects(context.Background(), r.option.BucketName, minio.ListObjectsOptions{})
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}

		file, err := parseObjectKey(object.Key)
		if err != nil {
			continue
		}

		if !(file.start > end || file.end < start) {
			if err := r.download(file.name); err != nil {
				return nil, err
			}

			filePath := path.Join(r.option.WorkDir, file.name)
			if err := r.dt.AddFile(filePath); err != nil {
				return nil, err
			}

			fileResult, err := r.dt.files[r.dt.filesIndexMap[filePath]].getRange(start, end, filter)
			if err != nil {
				return nil, err
			}

			result = append(result, fileResult...)
		}
	}

	return result, nil
}

type remoteFile struct {
	name  string
	start int64
	end   int64
}

func parseObjectKey(key string) (*remoteFile, error) {
	start, end, err := parseFileName(key)
	if err != nil {
		return nil, err
	}

	return &remoteFile{
		start: start, end: end,
		name: key,
	}, nil
}
