package config

import (
	"path/filepath"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type Config struct {
	ServerAddr      string `yaml:"serverAddr"`
}

func GetConfig() *Config {
	var config Config
	pwd, _ := os.Getwd()
	f, err := os.ReadFile(filepath.Join(pwd, "config/config.yaml"))
	if err != nil {
		log.Fatalf("read config error %v", err)
	}
	err = yaml.Unmarshal(f, &config)
	if err != nil {
		log.Fatalf("config unmarshal error: %v", err)
	}
	return &config
}
