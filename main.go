package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"./internal"
	"./service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	fpath string
	oldRedisNodeAddrList []string
)

func flags() {
	pflag.StringVar(&fpath, "config", "node.toml", "the toml config file")
	pflag.Parse()
}

func init() {
	flags()
}

func main() {
	internal.InitLoger(zapcore.InfoLevel)
	t := time.Now()
	cfg, err := internal.DecodeFile(fpath)
	if nil != err {
		internal.Panic("internal.DecodeFile err", zap.Error(err))
	}
	old_client, err := internal.NewRedisEngine(cfg.OldRedisAddrs[0], cfg.OldRedisPassword)

	old_is_cluster := strings.Split(strings.TrimSpace(old_client.Info(context.TODO(),"Cluster").Val()),":")
	service.OldIsCluster = old_is_cluster[1]
	if old_is_cluster[1] == "1" {
		service.OldRedisCluster, err = internal.NewRedisClusterEngine(cfg.OldRedisAddrs, cfg.OldRedisPassword)
		if nil != err {
			internal.Fatal("new redisCluster err: ",
				zap.Strings("addrs", cfg.OldRedisAddrs),
				zap.String("pass", cfg.OldRedisPassword),
				zap.Error(err))
		}
		oldRedisNodeAddrList = internal.GetRedisAddrs(cfg)
	}else{
		service.OldRedisSingle, err = internal.NewRedisEngine(cfg.OldRedisAddrs[0], cfg.OldRedisPassword)
		if nil != err {
			internal.Fatal("new redisSingle err: ",
				zap.Strings("addrs", cfg.OldRedisAddrs),
				zap.String("pass", cfg.OldRedisPassword),
				zap.Error(err))
		}
		oldRedisNodeAddrList = cfg.OldRedisAddrs
	}

	new_client, err := internal.NewRedisEngine(cfg.NewRedisAddrs[0], cfg.NewRedisPassword)
	new_is_cluster := strings.Split(strings.TrimSpace(new_client.Info(context.TODO(),"Cluster").Val()),":")
	service.NewIsCluster = new_is_cluster[1]

	if new_is_cluster[1] == "1" {
		service.NewRedisCluster, err = internal.NewRedisClusterEngine(cfg.NewRedisAddrs, cfg.NewRedisPassword)
		if nil != err {
			internal.Fatal("new redisCluster err: ",
				zap.Strings("addrs", cfg.OldRedisAddrs),
				zap.String("pass", cfg.OldRedisPassword),
				zap.Error(err))
		}
	}else{
		service.NewRedisSingle, err = internal.NewRedisEngine(cfg.NewRedisAddrs[0], cfg.NewRedisPassword)
		if nil != err {
			internal.Fatal("new redisSingle err: ",
				zap.Strings("addrs", cfg.NewRedisAddrs),
				zap.String("pass", cfg.NewRedisPassword),
				zap.Error(err))
		}
	}

	var wg sync.WaitGroup

	wg.Add(len(oldRedisNodeAddrList))
	for index := range oldRedisNodeAddrList {
		go func(addr string) {
			defer wg.Done()
			client, err := internal.NewRedisEngine(addr, cfg.OldRedisPassword)
			if nil != err {
				internal.Error("", zap.Error(err))
			} else {
				service.Handle(client, cfg)
				fmt.Println(client)
			}
		}(oldRedisNodeAddrList[index])
	}
	wg.Wait()

	internal.Info("success", zap.Duration("用时： ", time.Since(t)))
}
