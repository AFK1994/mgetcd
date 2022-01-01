package mgrpc

import (
	"github.com/go-kratos/kratos/v2/registry"
	"sync"
)

//轮询策略
type RoundRobinStrategy struct {
	i   int
	mtx sync.Mutex
}

func (r *RoundRobinStrategy) next(nodes []*registry.ServiceInstance) *registry.ServiceInstance {
	var node *registry.ServiceInstance
	r.mtx.Lock()
	node = nodes[r.i%len(nodes)]
	r.i++
	r.mtx.Unlock()
	return node
}
