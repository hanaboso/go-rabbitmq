package pkg

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSubscriber_Reconnect(t *testing.T) {
	ctx := createCtx()
	conn, _ := Connect(
		ctx,
		os.Getenv("RABBIT_DSN"),
	)

	sub, _ := NewSubscriber(ctx, conn)
	_ = sub.Close()
	ss := sub.(*subscriber)
	//time.Sleep(5 * time.Millisecond)
	_ = ss.handleReconnect(ctx)
}

func TestSubscriber_Connect(t *testing.T) {
	ctx := createCtx()
	conn, _ := Connect(
		ctx,
		os.Getenv("RABBIT_DSN"),
	)

	_, _ = NewSubscriber(ctx, conn)
	_ = conn.Close()

	time.Sleep(5 * time.Millisecond)
}

func createCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	return ctx
}
