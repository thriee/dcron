package driver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/libi/dcron/dlog"
)

type RedisZSetDriverV2 struct {
	c           *redis.Client
	serviceName string
	nodeID      string
	timeout     time.Duration
	logger      dlog.Logger
	started     bool

	// this context is used to define
	// the lifetime of this driver.
	runtimeCtx    context.Context
	runtimeCancel context.CancelFunc

	sync.Mutex
}

func newRedisZSetDriverV2(redisClient *redis.Client) *RedisZSetDriverV2 {
	rd := &RedisZSetDriverV2{
		c: redisClient,
		logger: &dlog.StdLogger{
			Log: log.Default(),
		},
		timeout: redisDefaultTimeout,
	}
	rd.started = false
	return rd
}

func (rd *RedisZSetDriverV2) Init(serviceName string, opts ...Option) {
	rd.serviceName = serviceName
	rd.nodeID = GetNodeId(serviceName)
	for _, opt := range opts {
		_ = rd.withOption(opt)
	}
}

func (rd *RedisZSetDriverV2) NodeID() string {
	return rd.nodeID
}

func (rd *RedisZSetDriverV2) GetNodes(ctx context.Context) (nodes []string, err error) {
	rd.Lock()
	defer rd.Unlock()
	sliceCmd := rd.c.ZRangeByScore(ctx, GetKeyPre(rd.serviceName), &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", TimePre(time.Now(), rd.timeout)),
		Max: "+inf",
	})
	if err = sliceCmd.Err(); err != nil {
		return nil, err
	} else {
		nodes = make([]string, len(sliceCmd.Val()))
		copy(nodes, sliceCmd.Val())
	}
	rd.logger.Infof("nodes=%v", nodes)
	return
}
func (rd *RedisZSetDriverV2) Start(_ context.Context) (err error) {
	rd.Lock()
	defer rd.Unlock()
	if rd.started {
		err = errors.New("this driver is started")
		return
	}
	rd.runtimeCtx, rd.runtimeCancel = context.WithCancel(context.TODO())
	rd.started = true
	// register
	err = rd.registerServiceNode()
	if err != nil {
		rd.logger.Errorf("register service error=%v", err)
		return
	}
	// heartbeat timer
	go rd.heartBeat()
	return
}
func (rd *RedisZSetDriverV2) Stop(_ context.Context) (err error) {
	rd.Lock()
	defer rd.Unlock()
	rd.runtimeCancel()
	rd.started = false
	return
}

func (rd *RedisZSetDriverV2) withOption(opt Option) (err error) {
	switch opt.Type() {
	case OptionTypeTimeout:
		rd.timeout = opt.(TimeoutOption).timeout
	case OptionTypeLogger:
		rd.logger = opt.(LoggerOption).logger
	}
	return
}

// private function

func (rd *RedisZSetDriverV2) heartBeat() {
	tick := time.NewTicker(rd.timeout / 2)
	for {
		select {
		case <-tick.C:
			if err := rd.registerServiceNode(); err != nil {
				rd.logger.Errorf("register service node error %+v", err)
			}
		case <-rd.runtimeCtx.Done():
			if err := rd.c.ZRem(context.Background(), GetKeyPre(rd.serviceName), rd.nodeID); err != nil {
				rd.logger.Errorf("unregister service node error %+v", err)
			}
			return
		}
	}
}

func (rd *RedisZSetDriverV2) registerServiceNode() error {
	ctx := context.Background()
	pipe := rd.c.Pipeline()
	members := &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: rd.nodeID,
	}

	zAddCmd := pipe.ZAdd(ctx, GetKeyPre(rd.serviceName), members)
	expireCmd := pipe.Expire(ctx, GetKeyPre(rd.serviceName), rd.timeout)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	if zAddCmd.Err() != nil {
		return zAddCmd.Err()
	}

	if expireCmd.Err() != nil {
		return expireCmd.Err()
	}

	return nil
}
