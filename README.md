用go-micro的方式重实现kratos的服务注册与发现，并提供调用go-micro/v2微服务的客户端

## 注册服务
```
func newApp(logger log.Logger, hs *http.Server, gs *grpc.Server) *kratos.App {
	regis := mgetcd.NewRegistry()
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			hs,
			gs,
		),
		kratos.Registrar(regis),
	)
}
```

## 调用go-micro/v2服务
```
grpcClient = mgrpc.NewMgetcdClient()

req := &Request{Name: "111"}
resp := &Response{}
err := grpcClient.Invoke(ctx, "go.micro.service.cservice", "Cservice.Call", req, resp)
if err != nil {
    return nil, err
}
```