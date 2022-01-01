package registry

import (
	"context"
	"errors"
	registry "github.com/go-kratos/kratos/v2/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type EtcdWatcher struct {
	w           clientv3.WatchChan
	client      *clientv3.Client
	timeout     time.Duration
	cancel      context.CancelFunc
	first       bool
	serviceName string
	stop        chan int
}

func newEtcdWatcher(ctx context.Context, serviceName string, client *clientv3.Client, timeout time.Duration) (registry.Watcher, error) {
	ctx1, cancel := context.WithCancel(ctx)

	return &EtcdWatcher{
		w:           client.Watch(ctx1, servicePath(serviceName), clientv3.WithPrefix(), clientv3.WithPrevKV()),
		client:      client,
		timeout:     timeout,
		cancel:      cancel,
		first:       true,
		serviceName: serviceName,
		stop:        make(chan int, 1),
	}, nil
}

func (w *EtcdWatcher) getInstance() ([]*registry.ServiceInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := w.client.Get(ctx, servicePath(w.serviceName), clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}
	items := make([]*registry.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if sn := decode(kv.Value); sn != nil {
			var endpoints []string
			for _, v := range sn.Nodes {
				endpoints = append(endpoints, v.Address)
			}
			s := &registry.ServiceInstance{
				Name:      sn.Name,
				Version:   sn.Version,
				Metadata:  sn.Metadata,
				Endpoints: endpoints,
			}

			items = append(items, s)
		}
	}
	return items, nil
}

func (w *EtcdWatcher) Next() ([]*registry.ServiceInstance, error) {
	var (
		err      error
		services []*registry.ServiceInstance
	)
	if w.first {
		w.first = false
		services, err = w.getInstance()
		if err != nil {
			return nil, err
		}
		return services, nil
	}
	for wresp := range w.w {
		select {
		case <-w.stop:
			return nil, errors.New("watcher done")
		default:
		}
		if wresp.Err() != nil {
			return nil, wresp.Err()
		}
		if wresp.Canceled {
			return nil, errors.New("could not get next")
		}
		services, err = w.getInstance()
		if err != nil {
			return nil, err
		}
		return services, nil
		//for _, ev := range wresp.Events {
		//	service := decode(ev.Kv.Value)
		//
		//	switch ev.Type {
		//	case clientv3.EventTypePut:
		//		endpoints := make([]string, len(service.Nodes))
		//		for _, v := range service.Nodes {
		//			endpoints = append(endpoints, v.Address)
		//		}
		//		sn := &registry.ServiceInstance{
		//			Name:      service.Name,
		//			Version:   service.Version,
		//			Metadata:  service.Metadata,
		//			Endpoints: endpoints,
		//		}
		//		services = append(services, sn)
		//		break
		//	case clientv3.EventTypeDelete:
		//		// get service from prevKv
		//		service = decode(ev.PrevKv.Value)
		//		endpoints := make([]string, len(service.Nodes))
		//		for _, v := range service.Nodes {
		//			endpoints = append(endpoints, v.Address)
		//		}
		//		sn := &registry.ServiceInstance{
		//			Name:      service.Name,
		//			Version:   service.Version,
		//			Metadata:  service.Metadata,
		//			Endpoints: endpoints,
		//		}
		//		services = append(services, sn)
		//		break
		//	}
		//
		//	if service == nil {
		//		continue
		//	}
		//}
	}
	return services, nil
}

func (w *EtcdWatcher) Stop() error {
	w.cancel()
	w.stop <- 1
	return nil
}
