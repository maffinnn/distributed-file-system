package config

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type Config struct {
	ServerAddr      string `yaml:"serverAddr"`
	ServerRootLevel string
}

func GetConfig() *Config {
	var config Config
	pwd, _ := os.Getwd()
	f, err := os.ReadFile(pwd + "/config/config.yaml")
	if err != nil {
		log.Fatalf("read config error %v", err)
	}
	err = yaml.Unmarshal(f, &config)
	if err != nil {
		log.Fatalf("config unmarshal error: %v", err)
	}
	config.ServerRootLevel = os.Getenv("ACCESSIBLE_ROOT_PATH")
	return &config
}
