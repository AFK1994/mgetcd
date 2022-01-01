package mgrpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"sync"
	"time"
)

//连接池
type connectionPool struct {
	sync.RWMutex
	pool     map[string]*streamsPool
	services map[string][]*registry.ServiceInstance
	ws       map[string]registry.Watcher
}

type streamsPool struct {
	//  链表头
	head *poolConn
	//  表长
	count int
}

type poolConn struct {
	//  grpc conn
	*grpc.ClientConn
	addr string

	//  pool and streams pool
	pool *connectionPool
	sp   *streamsPool

	created int64 //创建时间

	//  list
	pre  *poolConn
	next *poolConn
}

//维护缓存的服务列表
func (p *connectionPool) watch(service string, w registry.Watcher, logger *log.Helper) {
	for {
		ins, err := w.Next()
		if err != nil {
			logger.Errorf("watcher occur err:%s", err.Error())
			break
		}
		p.Lock()
		p.services[service] = ins
		p.Unlock()
		logger.Debugf("service [%s] update", service)
		if len(ins) == 0 {
			logger.Debugf("clear service [%s]", service)
			err = p.ClearByService(service)
			if err != nil {
				logger.Errorf("clear service [%s] occur err:%s", service, err.Error())
			}
			return
		}
	}
}

//根据服务名获取连接
func (p *connectionPool) GetService(service string, dic registry.Discovery, logger *log.Helper) ([]*registry.ServiceInstance, error) {
	var (
		err error
		ins []*registry.ServiceInstance
	)
	p.RLock()
	ins = p.services[service]
	p.RUnlock()
	if len(ins) > 0 {
		return ins, nil
	}
	services, err := dic.GetService(context.Background(), service)
	if err != nil {
		return services, err
	}
	if len(services) == 0 {
		return nil, errors.New(fmt.Sprintf("not found service [%s]", service))
	}
	p.Lock()
	defer p.Unlock()
	p.services[service] = services
	w, err := dic.Watch(context.Background(), service)
	if err != nil {
		return nil, err
	}
	_, ok := p.ws[service]
	if !ok {
		p.ws[service] = w
	}
	go p.watch(service, w, logger)
	return services, nil
}

//根据服务名获取连接
func (p *connectionPool) GetConnection(service, endpoint string, opts ...grpc.DialOption) (*poolConn, error) {
	var (
		err  error
		gc   *poolConn
		conn *grpc.ClientConn
	)
	now := time.Now().Unix()
	p.Lock()
	gcs, ok := p.pool[service]
	if !ok {
		gcs = &streamsPool{}
	}
	gc = gcs.head
	//遍历链表，直到返回一个可用连接，否则创建一个
	for gc != nil {
		//  检查连接状态
		switch gc.GetState() {
		case connectivity.Connecting:
			gc = gc.next
			continue
		case connectivity.Shutdown:
			next := gc.next
			removeConn(gc)
			gc = next
			continue
		case connectivity.TransientFailure:
			next := gc.next
			removeConn(gc)
			gc.ClientConn.Close()
			gc = next
			continue
		case connectivity.Ready:
		case connectivity.Idle:
		}

		//如果连接超过5分钟，则废弃
		if now-gc.created > 300 {
			next := gc.next
			removeConn(gc)
			gc.ClientConn.Close()
			gc = next
			continue
		}
		if gc.addr != endpoint {
			gc = gc.next
			continue
		}

		removeConn(gc)
		p.Unlock()
		return gc, nil
	}
	p.Unlock()
	conn, err = grpc.DialContext(context.Background(), endpoint, opts...)
	if err != nil {
		return nil, err
	}
	gc = &poolConn{
		ClientConn: conn,
		addr:       endpoint,
		pool:       p,
		sp:         gcs,
		created:    now,
	}

	p.Lock()
	fmt.Printf("new conn:%s\n", endpoint)
	p.pool[service] = gcs
	p.Unlock()
	return gc, nil
}

//连接使用完后放回连接池
func (p *connectionPool) ReleaseConnection(service string, gc *poolConn) error {
	p.Lock()
	defer p.Unlock()
	gcs, ok := p.pool[service]
	if !ok {
		return errors.New(fmt.Sprintf("pool not found service[%s]", service))
	}
	//添加到表头
	addConnAfter(gc, gcs.head)
	return nil
}

//关闭连接池
func (p *connectionPool) Close() error {
	p.RLock()
	defer p.Unlock()
	for _, gcs := range p.pool {
		var conn *poolConn
		conn = gcs.head
		for conn != nil && conn.ClientConn != nil {
			//连接可能已经关闭
			conn.ClientConn.Close()
		}
	}
	for _, w := range p.ws {
		w.Stop()
	}
	return nil
}

//清理指定服务连接池
func (p *connectionPool) ClearByService(service string) error {
	p.Lock()
	defer p.Unlock()
	delete(p.services, service)

	gcs, ok := p.pool[service]
	if ok {
		var conn *poolConn
		conn = gcs.head
		for conn != nil && conn.ClientConn != nil {
			conn.ClientConn.Close()
		}
	}
	delete(p.pool, service)

	w, ok := p.ws[service]
	if ok {
		w.Stop()
	}
	delete(p.ws, service)
	return nil
}

func removeConn(conn *poolConn) {
	if conn.pre != nil {
		conn.pre.next = conn.next
	}
	if conn.next != nil {
		conn.next.pre = conn.pre
		//移除的是表头
		if conn.next.pre == nil {
			conn.sp.head = conn.next
		}
	}
	conn.pre = nil
	conn.next = nil
	conn.sp.count--
	fmt.Printf("conn[%s].sp.count--:%d\n", conn.addr, conn.sp.count)
	if conn.sp.count == 0 {
		conn.sp.head = nil
	}
	return
}

func addConnAfter(conn *poolConn, after *poolConn) {
	if after != nil {
		conn.next = after
		after.pre = conn
	}
	conn.sp.head = conn
	conn.sp.count++
	fmt.Printf("conn[%s].sp.count++:%d\n", conn.addr, conn.sp.count)
	return
}
