package gcslock

import (
	"context"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func TestMutex_ContextLock(t *testing.T) {
	var (
		testBucketName          = "lock-bucket"
		testObjectName          = "lock-object"
		testDifferentObjectName = "lock-object-2"
	)

	tests := []struct {
		name           string
		initialObjects []fakestorage.Object
		wantErr        bool
	}{
		{
			name: "successful lock",
			initialObjects: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucketName,
						Name:       testDifferentObjectName,
					},
					Content: []byte("1"),
				},
			},
		},
		{
			name: "lock is already taken",
			initialObjects: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucketName,
						Name:       testObjectName,
					},
					Content: []byte("1"),
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gcsServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
				InitialObjects: tc.initialObjects,
			})
			if err != nil {
				t.Errorf("failed to start GCS server: %s", err)
			}
			defer gcsServer.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			m, err := New(ctx, testBucketName, testObjectName, gcsServer.Client())
			if err != nil {
				t.Errorf("failed to initialize mutex: %s", err)
			}

			if err := m.ContextLock(ctx); (err == nil) == tc.wantErr {
				t.Errorf("ContextLock(): %s", err)
			}
		})
	}

}

func TestMutex_ContextUnlockUnlock(t *testing.T) {
	var (
		testBucketName = "lock-bucket"
		testObjectName = "lock-object"
	)

	tests := []struct {
		name           string
		initialObjects []fakestorage.Object
	}{
		{
			name: "a lock file exists",
			initialObjects: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucketName,
						Name:       testObjectName,
					},
					Content: []byte("1"),
				},
			},
		},
		{
			name: "lock file does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gcsServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
				InitialObjects: tc.initialObjects,
			})
			if err != nil {
				t.Errorf("failed to start GCS server: %s", err)
			}
			defer gcsServer.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			m, err := New(ctx, testBucketName, testObjectName, gcsServer.Client())
			if err != nil {
				t.Errorf("failed to initialize mutex: %s", err)
			}

			if err := m.ContextUnlock(ctx); err != nil {
				t.Errorf("ContextUnLock(): %s", err)
			}
		})
	}
}
