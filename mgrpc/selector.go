package mgrpc

import "github.com/go-kratos/kratos/v2/registry"

type Selector struct {
	strategy Strategy
}

type Strategy interface {
	next([]*registry.ServiceInstance) *registry.ServiceInstance
}
