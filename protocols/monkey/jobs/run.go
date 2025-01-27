package jobs

import (
	"context"
	"time"

	"github.com/ipfs/go-log/v2"
)

var (
	logger = log.Logger("monkey.jobs.client")
)

func (c *Context) Run(ctx context.Context, ctxC context.CancelFunc) (err error) {
	c.ctx, c.ctxC = ctx, ctxC
	go c.startTimeout(ctx, ctxC)
	defer ctxC()

	if c.Job.Delay != nil {
		<-time.After(time.Duration(c.Job.Delay.Time) * time.Second)
	}

	if err = c.cloneAndSet(); err != nil {
		return err
	}

	contextHandler, err := c.Handler()
	if err != nil {
		return err
	}

	return contextHandler.handle()
}
