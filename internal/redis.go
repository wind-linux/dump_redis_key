package internal

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/go-redis/redis"
)

func NewRedisEngine(addr, pwd string) (*redis.Client, error) {
	opt := &redis.Options{
		Addr:     addr,
		Password: pwd,
		PoolSize: 10,
	}
	client := redis.NewClient(opt)
	str, err := client.Ping(context.TODO()).Result()
	log.Printf("redis ping: %#v", str)
	if nil != err {
		er := fmt.Errorf("NewRedisEngine [ addr %v pwd %v ] err: %v", addr, pwd, err)
		return client, er
	}
	return client, nil
}

func NewRedisClusterEngine(addrs []string, pwd string) (*redis.ClusterClient, error) {
	opt := &redis.ClusterOptions{
		Addrs:    addrs,
		Password: pwd,
		PoolSize: 10,
	}
	client := redis.NewClusterClient(opt)
	str, err := client.Ping(context.TODO()).Result()
	log.Printf("redis ping: %#v", str)
	if nil != err {
		er := fmt.Errorf("NewRedisEngine [ addr %v pwd %v ] err: %v", addrs, pwd, err)
		return client, er
	}
	return client, nil
}

func GetRedisAddrs(cfg Config) []string {
	client, err := NewRedisEngine(cfg.OldRedisAddrs[0], cfg.OldRedisPassword)
	if nil != err {
		panic(err)
	}
	nodes := decodeNodes(client)
	var addrs []string
	for _, node := range nodes {
		log.Println(node.Ip, node.Role)
		if strings.Contains(node.Role, "master") {
			addr := strings.Split(node.Ip, "@")[0]
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func decodeNodes(client *redis.Client) []redisNode {
	var rst = make([]redisNode, 0)
	nodes := client.ClusterNodes(context.TODO()).Val()
	log.Printf("nodes: %s", nodes)
	for _, node := range strings.Split(nodes, "\n") {
		strs := strings.Split(node, " ")
		if 3 < len(strs) {
			nd := redisNode{
				Id:   strs[0],
				Ip:   strs[1],
				Role: strs[2],
				//Unknow1: strs[3],
				//Unkonw2: strs[4],
				//Unkonw3: strs[5],
				//Unkonw4: strs[6],
				//Connect: strs[7],
				//NodeNum: strs[8],
			}
			rst = append(rst, nd)
		}
	}
	return rst
}

type redisNode struct {
	Id      string
	Ip      string
	Role    string
	Unknow1 string
	Unkonw2 string
	Unkonw3 string
	Unkonw4 string
	Connect string
	NodeNum string
}
