package main

import (
	"context"
	"io"
)

type contextReader struct {
	ctx context.Context
	r   io.ReadCloser
}

func (r *contextReader) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (r *contextReader) Close() error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		return r.r.Close()
	}
}

func NewContextReadCloser(ctx context.Context, reader io.ReadCloser) io.ReadCloser {
	return &contextReader{
		ctx: ctx,
		r:   reader,
	}
}
