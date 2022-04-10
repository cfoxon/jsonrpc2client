## A Simple and Fast JsonRPC2.0 Client

The simplicity of the structure and interaction of something like https://github.com/ybbus/jsonrpc is fantastic, but the use of the standard connection lib and reflect is a significant drag on performance in certain situations. Notable, it isn't ideal for something like downloading and parsing thousands of blocks from a blockchain.

This client exists primarily to facilitate a simple interface with blockchain api nodes. There is little error handling as it is expected that will be handled upstream when checking protocol specific issues - handle bytes once for maximum speed

### Usage

The general flow is to create a client:
```
maxConn := 2
maxBatch := 500
rpcClient := jsonrpc2client.NewClientWithOpts(endpoint, maxConn, maxBatch)
```

Create an RPC request (or batch of requests in this case):
```
    var jr2queries jsonrpc2client.RPCRequests
    for i, query := range queries {
        jr2query := &jsonrpc2client.RpcRequest{Method: query.method, JsonRpc: "2.0", Id: i, Params: query.params}
        jr2queries = append(jr2queries, jr2query)
    }
```

Then exec the RPC reqeuest. There are some options for single, batch, and batchFast. The "fast" option will return unprocessed bytes from the rpc server:
```
    resps, err :=rpcClient.CallBatchFast(jr2queries)
    if err != nil {
        return nil, err
    }
    data := doStuffWith(resps)
```
