package main

import (
	"github.com/FernandoCagale/gcp-pub-sub/worker"
	"github.com/joho/godotenv"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func init() {
	godotenv.Load()
}

func main() {
	pflag.StringP("message", "m", "message", "message of publish")
	pflag.StringP("topic", "t", "topic", "topic name")
	pflag.Parse()

	viper.BindPFlags(pflag.CommandLine)

	client := worker.NewWorker()

	topic, err := client.GetTopic(viper.GetString("topic"))
	if err != nil {
		panic(err)
	}

	if err := client.Publish(topic, viper.GetString("message")); err != nil {
		panic(err)
	}
}
