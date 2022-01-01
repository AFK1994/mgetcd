package registry

import (
	"encoding/json"
	"path"
	"strings"
)

const (
	Prefix = "/micro/registry/"
)

type Service struct {
	Name     string            `json:"name"`
	Version  string            `json:"version"`
	Metadata map[string]string `json:"metadata"`
	Nodes    []*Node           `json:"nodes"`
}

type Node struct {
	Id       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata"`
}

func encode(s *Service) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func decode(ds []byte) *Service {
	var s *Service
	json.Unmarshal(ds, &s)
	return s
}

func nodePath(s, id string) string {
	service := strings.Replace(s, "/", "-", -1)
	node := strings.Replace(id, "/", "-", -1)
	return path.Join(Prefix, service, node)
}

func servicePath(s string) string {
	return path.Join(Prefix, strings.Replace(s, "/", "-", -1))
}
