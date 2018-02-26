// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcproxy

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/proxy/grpcproxy/cache"
	"sync"
)

type zKvProxy struct {

	kvProxy
	remoteClients *[]*clientv3.Client
	numClusters int
}

func NewZKvProxy(c *clientv3.Client,  readerClients *[]*clientv3.Client) (pb.KVServer, <-chan struct{}) {

	kv := &zKvProxy{

		kvProxy:kvProxy{
			kv:c.KV,
			cache:cache.NewCache(cache.DefaultMaxEntries),
		},
		remoteClients:readerClients,
		numClusters:len(*readerClients),
	}
	donec := make(chan struct{})
	close(donec)
	return kv, donec
}

// 1. define result for remote access
type OpResponseStruct struct {
	resp clientv3.OpResponse
	err  error
}



func (p *zKvProxy) getRemoteResultList(ctx context.Context, r *pb.RangeRequest) (*[]*OpResponseStruct){

	resultList := make([]*OpResponseStruct, p.numClusters)

	// countdown latch
	var wg sync.WaitGroup
	wg.Add(p.numClusters)


	for i, remoteClientTemp := range *(p.remoteClients){

		index := i
		remoteClient := remoteClientTemp

		go func(){

			resp, err := remoteClient.KV.Do(ctx, RangeRequestToOp(r))
			resultList[index] = &OpResponseStruct{
				resp: resp,
				err: err,
			}

			wg.Done() // countdown
		}()
	}

	wg.Wait()

	return &resultList
}


func (p *zKvProxy) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if r.Serializable {
		resp, err := p.cache.Get(r)
		switch err {
		case nil:
			cacheHits.Inc()
			return resp, nil
		case cache.ErrCompacted:
			cacheHits.Inc()
			return nil, err
		}

		cachedMisses.Inc()
	}

	remoteResultList := *(p.getRemoteResultList(ctx, r))

	firstResponse := remoteResultList[0]

	if firstResponse.err != nil {
		return nil, firstResponse.err
	}

	rangeResult := firstResponse.resp.Get()

	// combine results
	for i := 1 ; i < len(remoteResultList); i++ {

		remoteResult := remoteResultList[i]

		if remoteResult.err != nil {
			return nil, remoteResult.err
		}

		rangeResult.Kvs = append(rangeResult.Kvs, remoteResult.resp.Get().Kvs...)
		rangeResult.Count += remoteResult.resp.Get().Count

	}

	// IMPORTANT!
	//TODO: implement LIMIT: SORT :MORE etc

	// cache linearizable as serializable
	req := *r
	req.Serializable = true
	gresp := (*pb.RangeResponse)(rangeResult)
	p.cache.Add(&req, gresp)
	cacheKeys.Set(float64(p.cache.Size()))

	return gresp, nil
}



