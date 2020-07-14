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
	pflag.StringP("topic", "t","topic", "topic name")
	pflag.StringSliceP("subscription", "s", []string{"sub-v1", "sub-v2"}, "subscription name")
	pflag.Parse()

	viper.BindPFlags(pflag.CommandLine)

	client := worker.NewWorker()

	topic, err := client.CreateTopic(viper.GetString("topic"))
	if err != nil {
		panic(err)
	}

	for _, name := range viper.GetStringSlice("subscription")  {
		if err := client.CreateSubscription(topic, name); err != nil {
			panic(err)
		}
	}
}