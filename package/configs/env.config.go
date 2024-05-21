package configs

import "github.com/spf13/viper"

type Config struct {
}

func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName(".env")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()

	if err != nil {
		panic(err)
	}

	err = viper.Unmarshal(&config)

	if err != nil {
		panic(err)
	}

	return
}
