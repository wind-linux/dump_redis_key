package internal

import (
	"fmt"
	"github.com/BurntSushi/toml"
)


type Config struct {
	OldRedisAddrs    []string
	OldRedisPassword string

	NewRedisAddrs    []string
	NewRedisPassword string

	DumpKeys		 []string
}

func DecodeFile(fpath string) (Config, error) {
	var config Config
	_, err := toml.DecodeFile(fpath, &config)
	if nil != err {
		er := fmt.Errorf("DecodeFile [ fpath %s ] err: %v", fpath, err)
		return config, er
	}
	return config, nil
}
