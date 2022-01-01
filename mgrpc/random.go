package mgrpc

import (
	"github.com/go-kratos/kratos/v2/registry"
	"math/rand"
)

//随机策略
type RandomStrategy struct {
}

func (r *RandomStrategy) next(nodes []*registry.ServiceInstance) *registry.ServiceInstance {
	var node *registry.ServiceInstance
	i := rand.Int31n(int32(len(nodes)))
	node = nodes[i]
	return node
}
