package config

import (
	"log"
	"os"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ServerAddr string `yaml:"serverAddr"`
}

func GetConfig() *Config {
	var config Config
	f, err := os.ReadFile("config.yaml")
    if err != nil {
        log.Fatalf("read config error %v", err)
    }
    err = yaml.Unmarshal(f, &config)
    if err != nil {
        log.Fatalf("config unmarshal error: %v", err)
    }
    return &config
}