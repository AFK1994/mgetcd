package registry

import (
	"context"
	"crypto/tls"
	"errors"
	log "github.com/go-kratos/kratos/v2/log"
	registry "github.com/go-kratos/kratos/v2/registry"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net"
	"sync"
	"time"
)

type EtcdRegister struct {
	client  *clientv3.Client
	options Options
	sync.RWMutex
	leaseId  clientv3.LeaseID
	register []string
	logger   *log.Helper
}

func NewRegistry(opts ...Option) *EtcdRegister {
	e := &EtcdRegister{
		options:  Options{},
		register: make([]string, 0),
		logger:   log.NewHelper(log.DefaultLogger),
	}
	configureRegistry(e, opts...)
	return e
}

func configureRegistry(e *EtcdRegister, opts ...Option) error {
	config := clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	for _, o := range opts {
		o(&e.options)
	}

	if e.options.Timeout <= 0 {
		e.options.Timeout = 5 * time.Second
	}
	if e.options.TTL <= 0 {
		e.options.TTL = 5 * time.Second
	}

	if e.options.Secure || e.options.TLSConfig != nil {
		tlsConfig := e.options.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		config.TLS = tlsConfig
	}

	var cAddrs []string

	for _, address := range e.options.Addrs {
		if len(address) == 0 {
			continue
		}
		addr, port, err := net.SplitHostPort(address)
		if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
			port = "2379"
			addr = address
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		} else if err == nil {
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		}
	}

	// if we got addrs then we'll update
	if len(cAddrs) > 0 {
		config.Endpoints = cAddrs
	}

	cli, err := clientv3.New(config)
	if err != nil {
		return err
	}
	e.client = cli
	return nil
}

func (r *EtcdRegister) heartBeat(ctx context.Context) {
	kac, err := r.client.KeepAlive(context.Background(), r.leaseId)
	if err != nil {
		r.logger.Errorf("KeepAlive occur err:%s", err.Error())
		return
	}
	for {
		select {
		case _, ok := <-kac:
			if !ok {
				continue
			}
			//r.logger.Debugf("heartBeat LeaseID:%d",msg.ID)
		}
	}
}

func (r *EtcdRegister) registerNode(ctx context.Context, s *Service, node *Node) error {
	if len(s.Nodes) == 0 || node == nil {
		r.logger.Errorf("Require at least one node")
		return errors.New("Require at least one node")
	}

	// missing lease, check if the key exists
	ctx1, cancel1 := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel1()

	// look for the existing key
	rsp, err := r.client.Get(ctx1, nodePath(s.Name, node.Id), clientv3.WithSerializable())
	if err != nil {
		return err
	}

	if len(rsp.Kvs) > 0 {
		r.logger.Errorf("node [%s] existed", nodePath(s.Name, node.Id))
		return errors.New("node existed")
	}

	if r.leaseId == 0 {
		service := &Service{
			Name:     s.Name,
			Version:  s.Version,
			Metadata: s.Metadata,
			Nodes:    []*Node{node},
		}
		var (
			lgr *clientv3.LeaseGrantResponse
			ttl int64
		)
		ttl = int64(r.options.TTL.Seconds())
		// get a lease used to expire keys since we have a ttl
		lgr, err = r.client.Grant(ctx, ttl)
		if err != nil {
			return err
		}
		if lgr == nil {
			return errors.New("get no lease")
		}
		ctx2, cancel2 := context.WithTimeout(ctx, r.options.Timeout)
		defer cancel2()
		// create an entry for the node
		r.logger.Debugf(nodePath(service.Name, node.Id))
		_, err = r.client.Put(ctx2, nodePath(service.Name, node.Id), encode(service), clientv3.WithLease(lgr.ID))
		if err != nil {
			return err
		}
		r.leaseId = lgr.ID
	}

	r.Lock()
	// save the service
	r.register = append(r.register, node.Id)
	r.Unlock()
	return nil
}

func (r *EtcdRegister) Register(ctx context.Context, service *registry.ServiceInstance) error {
	if len(service.Endpoints) == 0 {
		r.logger.Errorf("Require at least one node")
		return errors.New("Require at least one node")
	}

	var err error
	s := &Service{
		Name:     service.Name,
		Version:  service.Version,
		Metadata: service.Metadata,
	}

	// register each node individually
	for _, node := range service.Endpoints {
		n := &Node{
			Id:      service.Name + "-" + uuid.New().String(),
			Address: node,
		}
		s.Nodes = append(s.Nodes, n)
		err = r.registerNode(ctx, s, n)
		if err != nil {
			return err
		}
	}

	// heart beat
	go r.heartBeat(ctx)
	return nil
}

func (r *EtcdRegister) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	if len(service.Endpoints) == 0 {
		r.logger.Errorf("Require at least one node")
		return errors.New("Require at least one node")
	}

	r.Lock()
	defer r.Unlock()
	for _, nodeId := range r.register {
		// delete the service
		_, err := r.client.Delete(ctx, nodePath(service.Name, nodeId))
		if err != nil {
			return err
		}
	}
	r.register = make([]string, 0)
	r.leaseId = 0
	return nil
}

type EtcdDiscovery struct {
	client  *clientv3.Client
	options Options
	logger  *log.Helper
}

func NewDiscovery(opts ...Option) *EtcdDiscovery {
	e := &EtcdDiscovery{
		options: Options{},
		logger:  log.NewHelper(log.DefaultLogger),
	}
	err := configureDiscovery(e, opts...)
	if err != nil {
		e.logger.Errorf("configureDiscovery occur err:%s", err.Error())
	}
	return e
}

func configureDiscovery(e *EtcdDiscovery, opts ...Option) error {
	config := clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	for _, o := range opts {
		o(&e.options)
	}

	if e.options.Timeout <= 0 {
		e.options.Timeout = 5 * time.Second
	}
	if e.options.TTL <= 0 {
		e.options.TTL = 5 * time.Second
	}

	if e.options.Secure || e.options.TLSConfig != nil {
		tlsConfig := e.options.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		config.TLS = tlsConfig
	}

	var cAddrs []string

	for _, address := range e.options.Addrs {
		if len(address) == 0 {
			continue
		}
		addr, port, err := net.SplitHostPort(address)
		if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
			port = "2379"
			addr = address
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		} else if err == nil {
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		}
	}

	// if we got addrs then we'll update
	if len(cAddrs) > 0 {
		config.Endpoints = cAddrs
	}

	cli, err := clientv3.New(config)
	if err != nil {
		return err
	}
	e.client = cli
	return nil
}

func (r *EtcdDiscovery) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	ctx1, cancel1 := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel1()

	rsp, err := r.client.Get(ctx1, servicePath(serviceName), clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		r.logger.Errorf("Require at least one node")
		return nil, errors.New("service not found")
	}

	serviceMap := map[string]*Service{}

	for _, n := range rsp.Kvs {
		if sn := decode(n.Value); sn != nil {
			s, ok := serviceMap[sn.Version]
			if !ok {
				s = &Service{
					Name:     sn.Name,
					Version:  sn.Version,
					Metadata: sn.Metadata,
				}
				serviceMap[s.Version] = s
			}

			s.Nodes = append(s.Nodes, sn.Nodes...)
		}
	}

	services := make([]*registry.ServiceInstance, 0, len(serviceMap))
	for _, service := range serviceMap {
		endpoints := make([]string, len(service.Nodes))
		for i, v := range service.Nodes {
			endpoints[i] = v.Address
		}
		sn := &registry.ServiceInstance{
			Name:      service.Name,
			Version:   service.Version,
			Metadata:  service.Metadata,
			Endpoints: endpoints,
		}
		services = append(services, sn)
	}

	return services, nil
}

func (r *EtcdDiscovery) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	w, err := newEtcdWatcher(ctx, serviceName, r.client, r.options.Timeout)
	return w, err
}
