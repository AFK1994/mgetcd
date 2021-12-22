package mgrpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/AFK1994/mgetcd/registry"
	"github.com/go-kratos/kratos/v2/log"
	krareg "github.com/go-kratos/kratos/v2/registry"
	"google.golang.org/grpc"
	"strings"
	"sync"
)

type MgetcdClient struct {
	dic      *registry.EtcdDiscovery
	selector *Selector
	sync.RWMutex
	pool    map[string][]*grpc.ClientConn
	options Options
	logger  *log.Helper
	w       krareg.Watcher
}

func NewMgetcdClient(opts ...Option) *MgetcdClient {
	e := &MgetcdClient{
		pool:   make(map[string][]*grpc.ClientConn),
		logger: log.NewHelper(log.DefaultLogger),
	}
	configure(e, opts...)
	return e
}

func configure(e *MgetcdClient, opts ...Option) error {
	for _, o := range opts {
		o(&e.options)
	}
	dic := registry.NewDiscovery(
		registry.Addrs(e.options.Addrs...),
		registry.Timeout(e.options.Timeout),
		registry.Secure(e.options.Secure),
		registry.TLSConfig(e.options.TLSConfig),
		registry.TTLConfig(e.options.TTL),
	)
	e.dic = dic

	selector := &Selector{strategy: e.options.Strategy}
	if selector.strategy == nil {
		selector.strategy = &RandomStrategy{}
	}
	e.selector = selector
	return nil
}

func (c *MgetcdClient) Invoke(ctx context.Context, service, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	services, err := c.dic.GetService(ctx, service)
	if err != nil {
		return err
	}
	if len(services) == 0 {
		return errors.New("not found service")
	}
	s := c.selector.strategy.next(services)

	var gc *grpc.ClientConn
	c.RLock()
	gcs, ok := c.pool[service]
	c.RUnlock()
	//TODO connections
	if ok && len(gcs) > 0 {
		gc = gcs[0]
	}
	if gc == nil {
		gc, err = grpc.Dial(s.Endpoints[0], grpc.WithInsecure())
		if err != nil {
			return err
		}
	}
	err = gc.Invoke(ctx, MethodToGRPC(service, method), args, reply, opts...)
	if err != nil {
		gc.Close()
		return err
	}
	c.Lock()
	gcs = append(gcs, gc)
	c.pool[service] = gcs
	c.Unlock()
	return nil
}

func (c *MgetcdClient) Close() error {
	if c.dic != nil {
		err := c.dic.Close()
		if err != nil {
			return err
		}
	}
	c.w.Stop()
	return nil
}

func MethodToGRPC(service, method string) string {
	// no method or already grpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	// assume method is Foo.Bar
	mParts := strings.Split(method, ".")
	if len(mParts) != 2 {
		return method
	}

	if len(service) == 0 {
		return fmt.Sprintf("/%s/%s", mParts[0], mParts[1])
	}

	// return /pkg.Foo/Bar
	return fmt.Sprintf("/%s.%s/%s", service, mParts[0], mParts[1])
}
