package service

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"../internal"
	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

/*
   作者：Jason
   创建日期：2018/1/30
   编辑日期：2018/1/30
   功能描述：
   修改详细描述
*/

var (
	KeyCount        int64
	NewRedisCluster *redis.ClusterClient
	OldRedisCluster *redis.ClusterClient
	NewRedisSingle  *redis.Client
	OldRedisSingle  *redis.Client
	NewIsCluster	string
	OldIsCluster	string
)

func init() {
	go monitoKey()
}

// 监控key
func monitoKey() {
	start := time.Now()
	t := time.NewTimer(time.Second * 5)
	for {
		select {
		case <-t.C:
			log.Printf("扫描了 %d 个Key, 用了 %v ", KeyCount, time.Since(start))
			t.Reset(time.Second * 5)
		}
	}
}

func Handle(client *redis.Client, cfg internal.Config) {

	for _, dpkey := range cfg.DumpKeys {
		var cursor uint64
		internal.Info(dpkey)

		for {

			keys, cs, err := client.Scan(context.TODO(),cursor, dpkey + "*", 1000).Result()
			if nil != err {
				internal.Error("Handle Scan err", zap.Error(err))
				return
			}

			if l := len(keys); l > 0 {
				atomic.AddInt64(&KeyCount, int64(l))
				DumpKey(keys)
			}

			if cs == 0 {
				break
			} else {
				cursor = cs
			}
		}
	}

}

func DumpKey(keyList []string) {
	for _, key := range keyList {
		if OldIsCluster == "1" {
			dumpVal, err := OldRedisCluster.Dump(context.TODO(),key).Result()
			if nil != err {
				internal.Error("DumpKey err",
					zap.String("key", key),
					zap.Error(err))
				continue
			}
			RestoreKey(key, dumpVal)

		} else {
			dumpVal, err := OldRedisSingle.Dump(context.TODO(),key).Result()
			if nil != err {
				internal.Error("DumpKey err",
					zap.String("key", key),
					zap.Error(err))
				continue
			}
			RestoreKey(key, dumpVal)

		}

	}

}

func RestoreKey(key, val string) {

	if NewIsCluster == "1" {
		result, err := NewRedisCluster.RestoreReplace(context.TODO(),key, 0, val).Result()
		if nil != err {
			internal.Error("RestoreKey err",
				zap.String("key", key),
				zap.String("ok", result),
				zap.Error(err))
		}
	} else {
		result, err := NewRedisSingle.RestoreReplace(context.TODO(),key, 0, val).Result()
		if nil != err {
			internal.Error("RestoreKey err",
				zap.String("key", key),
				zap.String("ok", result),
				zap.Error(err))
		}
	}

}
