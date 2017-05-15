package config

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type Header struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type HastyConfig struct {
	Authentication struct {
		URL     string   `yaml:"url"`
		Path    string   `yaml:"path"`
		Headers []Header `yaml:"headers"`
	}
}

func LoadConfig() (HastyConfig, error) {
	t := HastyConfig{}
	data, dataErr := ioutil.ReadFile(".config/hasty")
	if dataErr != nil {
		return t, dataErr
	}
	parseErr := yaml.Unmarshal(data, &t)
	if parseErr != nil {
		return t, parseErr
	}

	return t, nil
}
