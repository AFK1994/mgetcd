package mgrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/AFK/mgetcd/registry"
	"github.com/go-kratos/kratos/v2/log"
	krareg "github.com/go-kratos/kratos/v2/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"strings"
	"sync"
	"time"
)

type MgetcdClient struct {
	dic      *registry.EtcdDiscovery
	selector *Selector
	sync.RWMutex
	pool    *connectionPool
	options Options
	logger  *log.Helper
}

func NewMgetcdClient(opts ...Option) *MgetcdClient {
	e := &MgetcdClient{
		pool: &connectionPool{
			pool:     make(map[string]*streamsPool, 0),
			services: make(map[string][]*krareg.ServiceInstance, 0),
			ws:       make(map[string]krareg.Watcher, 0),
		},
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
	services, err := c.pool.GetService(service, c.dic, c.logger)
	if err != nil {
		return err
	}
	if len(services) == 0 {
		return errors.New(fmt.Sprintf("not found service [%s]", service))
	}

	// 检查是否已经存在deadline
	_, ok := ctx.Deadline()
	if !ok {
		// 没有 deadline 创建5秒超时
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
	}

	call := func() error {
		s := c.selector.strategy.next(services)

		var gc *poolConn
		gc, err = c.pool.GetConnection(service, s.Endpoints[0], c.secure(s.Endpoints[0]))
		if err != nil {
			c.logger.Errorf("get grpc connection occur err:%s", err.Error())
			return err
		}
		if gc == nil {
			c.logger.Errorf("not found connection")
			return errors.New("not found connection")
		}

		err = gc.Invoke(ctx, MethodToGRPC(service, method), args, reply, opts...)
		if err != nil {
			gc.Close()
			return err
		}
		err = c.pool.ReleaseConnection(service, gc)
		if err != nil {
			return err
		}
		return nil
	}

	//重试3次
	for i := 0; i < 3; i++ {
		select {
		case <-ctx.Done(): //超时立即返回
			return errors.New(fmt.Sprintf("invoke [%s].[%s]time out", service, method))
		default:
		}
		err = call()
		if i == 2 {
			//认为服务全部不可用，清空缓存服务列表、连接
			err1 := c.pool.ClearByService(service)
			if err1 != nil {
				c.logger.Errorf("clear service [%s] occur err:%s", service, err1.Error())
			}
			return err
		}
		if err != nil {
			c.logger.Errorf("client call occur err:%s", err.Error())
			err = nil
			continue
		}
		return nil
	}
	return nil
}

func (c *MgetcdClient) Close() error {
	if c.dic != nil {
		err := c.dic.Close()
		if err != nil {
			return err
		}
	}
	err := c.pool.Close()
	return err
}

// 是否需要安全验证
func (c *MgetcdClient) secure(addr string) grpc.DialOption {
	// 是否有 tls config
	if c.options.TLSConfig != nil {
		tls := c.options.TLSConfig
		creds := credentials.NewTLS(tls)
		return grpc.WithTransportCredentials(creds)
	}

	// 默认 config
	tlsConfig := &tls.Config{}
	defaultCreds := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))

	// 检查是否是 https
	if strings.HasPrefix(addr, "https://") {
		return defaultCreds
	}

	// 地址中是否指定端口
	_, port, err := net.SplitHostPort(addr)
	if port == "443" {
		return defaultCreds
	} else if err != nil && strings.Contains(err.Error(), "missing port in address") {
		return defaultCreds
	}

	// 没有安全认证
	return grpc.WithInsecure()
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
