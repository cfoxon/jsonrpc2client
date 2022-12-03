package jsonrpc2client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/valyala/fasthttp"
)

type RPCRequests []*RpcRequest

type RpcRequest struct {
	JsonRpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type RpcResponses []*RpcResponse

type RpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RpcError       `json:"error,omitempty"`
	ID      int             `json:"id"`
}

type RpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcClient struct {
	endpoint       string
	httpClient     *fasthttp.Client
	MaxConnections int
	MaxBatchSize   int
}

func (client *rpcClient) newRequest(req interface{}) (*fasthttp.Request, error) {

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	request := fasthttp.AcquireRequest()
	request.SetBody(body)
	request.Header.SetMethod("POST")
	request.Header.SetContentType("application/json")
	request.SetRequestURI(client.endpoint)
	return request, nil
}

func (client *rpcClient) CallBatch(requests RPCRequests) (RpcResponses, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}

	for i, req := range requests {
		req.Id = i
		req.JsonRpc = "2.0"
	}
	return client.doBatchCall(requests)
}

func (client *rpcClient) CallBatchRaw(requests RPCRequests) (RpcResponses, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}
	return client.doBatchCall(requests)
}

func (client *rpcClient) CallBatchFast(requests RPCRequests) ([][]byte, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}
	return client.doFastBatchCall(requests)
}

func NewClient(endpoint string) *rpcClient {
	return NewClientWithOpts(endpoint, 1, 4)
}

func NewClientWithOpts(endpoint string, maxConn int, maxBatch int) *rpcClient {
	return &rpcClient{
		endpoint:       endpoint,
		httpClient:     &fasthttp.Client{DialDualStack: true},
		MaxConnections: maxConn,
		MaxBatchSize:   maxBatch,
	}
}

func (client *rpcClient) CallRaw(request *RpcRequest) (*RpcResponse, error) {
	return client.doCall(request)
}

func (client *rpcClient) doBatchCall(rpcRequests []*RpcRequest) ([]*RpcResponse, error) {
	reqs := (len(rpcRequests) / client.MaxBatchSize) + 1
	pendingRpcReqs := make(chan RPCRequests, reqs)

	batch := RPCRequests{}

	for i, p := range rpcRequests {
		batch = append(batch, p)

		if i%client.MaxBatchSize == 0 && i > 0 {
			var pendingBatch RPCRequests
			pendingBatch = batch
			pendingRpcReqs <- pendingBatch
			batch = nil
		}
	}

	if len(batch) > 0 {
		var pendingBatch RPCRequests
		pendingBatch = batch
		pendingRpcReqs <- pendingBatch
		batch = nil
	}

	close(pendingRpcReqs)

	numWorkers := client.MaxConnections
	if reqs < numWorkers {
		numWorkers = reqs
	}

	var wait sync.WaitGroup
	wait.Add(numWorkers)

	rpcResponses := RpcResponses{}

	work := func(rpcRequest RPCRequests) {
		rpcResponse := RpcResponses{}
		httpRequest, err := client.newRequest(rpcRequest)
		if err != nil {
			log.Printf("%v", err)
			rpcResponse = RpcResponses{&RpcResponse{Error: &RpcError{Message: err.Error()}}}
			rpcResponses = append(rpcResponses, rpcResponse...)
			return
		}
		httpRequest.Header.Set("Accept-Encoding", "gzip")
		res := fasthttp.AcquireResponse()

		if err2 := fasthttp.Do(httpRequest, res); err != nil {
			log.Printf("%v", err2)
			rpcResponse = RpcResponses{&RpcResponse{Error: &RpcError{Message: err2.Error()}}}
			rpcResponses = append(rpcResponses, rpcResponse...)
			return
		}
		fasthttp.ReleaseRequest(httpRequest)

		contentEncoding := res.Header.Peek("Content-Encoding")
		var body []byte
		if bytes.EqualFold(contentEncoding, []byte("gzip")) {
			body, _ = res.BodyGunzip()
		} else {
			body = res.Body()
		}

		if err3 := json.Unmarshal(body, &rpcResponse); err != nil {
			log.Printf("%v", err)
			rpcResponse = RpcResponses{&RpcResponse{Error: &RpcError{Message: err3.Error()}}}
			rpcResponses = append(rpcResponses, rpcResponse...)
			return
		}

		if len(rpcResponse) > 0 {
			rpcResponses = append(rpcResponses, rpcResponse...)
		}
		fasthttp.ReleaseResponse(res)
	}

	worker := func(ch <-chan RPCRequests) {
		defer wait.Done()

		for j := range ch {
			work(j)
		}
	}
	for i := 0; i < numWorkers; i++ {
		go worker(pendingRpcReqs)
	}

	wait.Wait()
	return rpcResponses, nil
}

func (client *rpcClient) doFastBatchCall(rpcRequests []*RpcRequest) ([][]byte, error) {
	reqs := (len(rpcRequests) / client.MaxBatchSize) + 1
	pendingRpcReqs := make(chan RPCRequests, reqs)

	var batch RPCRequests
	for i, p := range rpcRequests {
		batch = append(batch, p)
		if i%client.MaxBatchSize == 0 && i > 0 {
			var pendingBatch RPCRequests
			pendingBatch = batch
			pendingRpcReqs <- pendingBatch
			batch = nil
		}
	}

	if len(batch) > 0 {
		var pendingBatch RPCRequests
		pendingBatch = batch
		pendingRpcReqs <- pendingBatch
		batch = nil
	}
	close(pendingRpcReqs)

	numWorkers := client.MaxConnections
	if reqs < numWorkers {
		numWorkers = reqs
	}

	resc := make(chan []byte)
	work := func(rpcRequest RPCRequests) {
		httpRequest, err := client.newRequest(rpcRequest)
		if err != nil {
			log.Printf("%v", err)
			respErr := RpcResponses{&RpcResponse{Error: &RpcError{Message: err.Error()}}}
			respErrB, _ := json.Marshal(respErr)
			resc <- respErrB
			return
		}

		res := fasthttp.AcquireResponse()
		httpRequest.Header.Set("Accept-Encoding", "gzip")

		if err2 := fasthttp.Do(httpRequest, res); err != nil {
			log.Printf("%v", err2)
			respErr := RpcResponses{&RpcResponse{Error: &RpcError{Message: err2.Error()}}}
			respErrB, _ := json.Marshal(respErr)
			resc <- respErrB
			return
		}
		fasthttp.ReleaseRequest(httpRequest)

		contentEncoding := res.Header.Peek("Content-Encoding")
		var body []byte
		if bytes.EqualFold(contentEncoding, []byte("gzip")) {
			body, _ = res.BodyGunzip()
		} else {
			body = res.Body()
		}

		if len(body) > 0 {
			resc <- body
		}
		fasthttp.ReleaseResponse(res)
	}

	worker := func(ch <-chan RPCRequests) {
		for j := range ch {
			work(j)
		}
	}
	for i := 0; i < numWorkers; i++ {
		go worker(pendingRpcReqs)
	}

	var rpcResponses [][]byte
	for i := 0; i < numWorkers; i++ {
		b := <-resc
		rpcResponses = append(rpcResponses, b)
	}
	close(resc)

	return rpcResponses, nil
}

func (client *rpcClient) doCall(RPCRequest *RpcRequest) (*RpcResponse, error) {
	httpRequest, err := client.newRequest(RPCRequest)
	if err != nil {
		return nil, fmt.Errorf("rpc batch call on %v: %v", client.endpoint, err.Error())
	}
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	res := fasthttp.AcquireResponse()

	if err := fasthttp.Do(httpRequest, res); err != nil {
		return nil, err
	}
	fasthttp.ReleaseRequest(httpRequest)

	contentEncoding := res.Header.Peek("Content-Encoding")
	var body []byte
	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		body, _ = res.BodyGunzip()
	} else {
		body = res.Body()
	}

	rpcResponse := &RpcResponse{}
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	fasthttp.ReleaseResponse(res)
	return rpcResponse, nil
}

func (client *rpcClient) doFastCall(RPCRequest *RpcRequest) (*RpcResponse, error) {
	httpRequest, err := client.newRequest(RPCRequest)
	if err != nil {
		return nil, fmt.Errorf("rpc batch call on %v: %v", client.endpoint, err.Error())
	}
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	res := fasthttp.AcquireResponse()

	if err := fasthttp.Do(httpRequest, res); err != nil {
		return nil, err
	}
	fasthttp.ReleaseRequest(httpRequest)

	contentEncoding := res.Header.Peek("Content-Encoding")
	var body []byte
	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		body, _ = res.BodyGunzip()
	} else {
		body = res.Body()
	}

	rpcResponse := &RpcResponse{}
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	fasthttp.ReleaseResponse(res)
	return rpcResponse, nil
}
