package gcslock

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/storage"
)

type ContextLocker interface {
	sync.Locker
	ContextLock(ctx context.Context) error
	ContextUnlock(ctx context.Context) error
}

type mutex struct {
	bucket string
	object string
	client *storage.Client
}

var _ ContextLocker = (*mutex)(nil)

// Lock waits indefinitely to acquire a mutex.
func (m *mutex) Lock() {
	_ = m.ContextLock(context.Background())
}

// ContextLock waits to acquire a mutex with timeout governed by passed context.
func (m *mutex) ContextLock(ctx context.Context) error {
	w := m.client.Bucket(m.bucket).Object(m.object).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.ContentType = "text/plain"

	backoff := 10 * time.Millisecond
	for {
		_, err := w.Write([]byte("1"))

		if err == nil {
			if err := w.Close(); err == nil {
				return nil
			}
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unlock waits indefinitely to release a mutex.
func (m *mutex) Unlock() {
	_ = m.ContextUnlock(context.Background())
}

// ContextUnlock waits to release a mutex with timeout governed by passed context.
func (m *mutex) ContextUnlock(ctx context.Context) error {
	o := m.client.Bucket(m.bucket).Object(m.object)

	backoff := 10 * time.Millisecond
	for {
		err := o.Delete(ctx)
		if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
			return nil
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// New creates a GCS-based sync.Locker.
// It uses Application Default Credentials to make authenticated requests
// to Google Cloud Storage. See the DefaultClient function of the
// golang.org/x/oauth2/google package for App Default Credentials details.
//
// If ctx argument is nil, context.Background is used.
func New(ctx context.Context, bucket, object string, client *storage.Client) (ContextLocker, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	m := &mutex{
		bucket: bucket,
		object: object,
		client: client,
	}

	if m.client == nil {
		cl, err := storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		m.client = cl
	}

	return m, nil
}
