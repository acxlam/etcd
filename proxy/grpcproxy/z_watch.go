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
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type zWatchProxy struct {
	numClusters    int
	watchProxies   *[]*watchProxy
	pendingWatches *sync.WaitGroup
}

func NewZWatchProxy(readerClients *[]*clientv3.Client) (pb.WatchServer, <-chan struct{}) {

	numClusters := len(*readerClients)

	watchProxies := make([]*watchProxy, numClusters, numClusters)

	ch := make(chan struct{})

	var pendingReq = sync.WaitGroup{}

	for i, c := range *readerClients {
		cctx, cancel := context.WithCancel(c.Ctx())

		wp := &watchProxy{
			cw:     c.Watcher,
			ctx:    cctx,
			leader: newLeader(c.Ctx(), c.Watcher),

			kv: c.KV, // for permission checking
		}

		watchProxies[i] = wp

		wp.ranges = newWatchRanges(wp)

		go func() {
			defer close(ch)
			<-wp.leader.stopNotify()
			wp.mu.Lock()
			select {
			case <-wp.ctx.Done():
			case <-wp.leader.disconnectNotify():
				cancel()
			}
			<-wp.ctx.Done()
			wp.mu.Unlock()
			pendingReq.Wait()
			wp.ranges.stop()
		}()
	}

	return &zWatchProxy{
		watchProxies:   &watchProxies,
		numClusters:    numClusters,
		pendingWatches: &pendingReq,
	}, ch
}

type watchProxyStreamHub struct {
	numClusters       int
	stream            pb.Watch_WatchServer
	watchCh           chan *pb.WatchResponse
	watchProxyStreams []*watchProxyStream
	ctx               context.Context
	mu                sync.Mutex
}

type watchProxyInfo struct {
	ranges   *watchRanges
	watchers map[int64]*watcher
	cancel   context.CancelFunc
	kv       clientv3.KV
}

func (zwp *zWatchProxy) Watch(stream pb.Watch_WatchServer) (err error) {

	watchProxyStreams := make([]*watchProxyStream, zwp.numClusters, zwp.numClusters)

	watchCh := make(chan *pb.WatchResponse, 1024)

	ctx, cancel := context.WithCancel(stream.Context())

	anyLeaderLostC := make(chan struct{})
	anyDisconnectNotifyC := make(chan struct{})

	for i, wp := range *(*zwp).watchProxies {

		wp.mu.Lock()
		select {
		case <-wp.ctx.Done():
			wp.mu.Unlock()
			select {
			case <-wp.leader.disconnectNotify():
				return grpc.ErrClientConnClosing
			default:
				return wp.ctx.Err()
			}
		default:
			plog.Infof("ranged")
		}

		wp.mu.Unlock()

		wps := &watchProxyStream{
			ranges:   wp.ranges,
			watchers: make(map[int64]*watcher),
			stream:   stream,
			watchCh:  watchCh,
			cancel:   cancel,
			ctx:      ctx,
			kv:       wp.kv,
		}

		watchProxyStreams[i] = wps

		var lostLeaderC <-chan struct{}
		if md, ok := metadata.FromOutgoingContext(stream.Context()); ok {
			v := md[rpctypes.MetadataRequireLeaderKey]
			if len(v) > 0 && v[0] == rpctypes.MetadataHasLeader {
				lostLeaderC = wp.leader.lostNotify()
				// if leader is known to be lost at creation time, avoid
				// letting events through at all
				select {
				case leaderLostEvent := <-lostLeaderC:
					anyLeaderLostC <- leaderLostEvent
					zwp.pendingWatches.Done()
					return rpctypes.ErrNoLeader
				default:
				}
			}
		}

	}

	watchProxyStreamHub := watchProxyStreamHub{
		watchCh:           watchCh,
		numClusters:       zwp.numClusters,
		stream:            stream,
		watchProxyStreams: watchProxyStreams,
		ctx:               ctx,
	}

	zwp.pendingWatches.Add(1)

	// since all goroutines will only terminate after Watch() exits.
	stopc := make(chan struct{}, 3)
	go func() {
		defer func() { stopc <- struct{}{} }()
		watchProxyStreamHub.recvLoop()
	}()
	go func() {
		defer func() { stopc <- struct{}{} }()
		watchProxyStreamHub.sendLoop()
	}()

	// tear down watch if leader goes down or entire watch proxy is terminated
	go func() {

		plog.Info("Before stop notify")

		defer func() { stopc <- struct{}{} }()
		select {
		case <-anyLeaderLostC:
		case <-ctx.Done():
		case <-watchProxyStreamHub.ctx.Done():
		}

		plog.Info("Event of any stop")

	}()

	for _, wp := range *(*zwp).watchProxies {

		plog.Info("Any event wait")

		go func(wp *watchProxy) {

			select {
			case leaderLostEvent := <-wp.leader.lostNotify():
				anyLeaderLostC <- leaderLostEvent
			case disconnectEvent := <-wp.leader.disconnectNotify():
				anyDisconnectNotifyC <- disconnectEvent
			}

		}(wp)

		plog.Info("Any event found")

	}

	<-stopc
	cancel()

	// recv/send may only shutdown after function exits;
	// goroutine notifies proxy that stream is through
	go func() {
		<-stopc
		<-stopc
		watchProxyStreamHub.close()
		zwp.pendingWatches.Done()
	}()

	select {
	case <-anyLeaderLostC:
		return rpctypes.ErrNoLeader
	case <-anyDisconnectNotifyC:
		return grpc.ErrClientConnClosing
	default:
		return watchProxyStreamHub.ctx.Err()
	}

}

// TODO: use gofunc for parrallel
func (wpsm *watchProxyStreamHub) close() {

	var wg sync.WaitGroup

	wpsm.mu.Lock()
	for _, wps := range wpsm.watchProxyStreams {
		wps.cancel()
		wg.Add(len(wps.watchers))
		for _, wpsw := range wps.watchers {
			go func(w *watcher) {
				wps.ranges.delete(w)
				wg.Done()
			}(wpsw)
		}
		wps.watchers = nil

		wg.Wait()
	}
	wpsm.mu.Unlock()

	close(wpsm.watchCh)

}

func (wpsm *watchProxyStreamHub) checkPermissionForWatch(key, rangeEnd []byte) error {
	if len(key) == 0 {
		// If the length of the key is 0, we need to obtain full range.
		// look at clientv3.WithPrefix()
		key = []byte{0}
		rangeEnd = []byte{0}
	}
	req := &pb.RangeRequest{
		Serializable: true,
		Key:          key,
		RangeEnd:     rangeEnd,
		CountOnly:    true,
		Limit:        1,
	}

	var wg sync.WaitGroup

	wg.Add(wpsm.numClusters)

	resultChan := make(chan interface{}, wpsm.numClusters)

	defer close(resultChan)

	for _, wps := range wpsm.watchProxyStreams {

		go func(wps *watchProxyStream) {
			res, err := wps.kv.Do(wps.ctx, RangeRequestToOp(req))

			if err != nil {
				resultChan <- err
			}else {
				resultChan <- res
			}


			wg.Done()
		}(wps)

	}

	wg.Wait()

	for i := 0; i < wpsm.numClusters; i++ {

		result := <-resultChan
		if permissionError, ok := result.(error); ok {
			return permissionError
		}
	}

	return nil
}

func (wpsm *watchProxyStreamHub) recvLoop() error {

	for {
		req, err := wpsm.stream.Recv()
		if err != nil {
			return err
		}

		plog.Info("receive request %v", req)

		switch uv := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			cr := uv.CreateRequest

			if wpsm.numClusters > 1 && cr.StartRevision != 0 {
				cr.StartRevision = 0
			}

			if err = wpsm.checkPermissionForWatch(cr.Key, cr.RangeEnd); err != nil && err == rpctypes.ErrPermissionDenied {
				// Return WatchResponse which is caused by permission checking if and only if
				// the error is permission denied. For other errors (e.g. timeout or connection closed),
				// the permission checking mechanism should do nothing for preserving error code.
				wpsm.watchCh <- &pb.WatchResponse{Header: &pb.ResponseHeader{}, WatchId: -1, Created: true, Canceled: true}
				continue
			}

			for _, wps := range wpsm.watchProxyStreams {

				w := &watcher{
					wr:  watchRange{string(cr.Key), string(cr.RangeEnd)},
					id:  wps.nextWatcherID,
					wps: wps,

					nextrev:  cr.StartRevision,
					progress: cr.ProgressNotify,
					prevKV:   cr.PrevKv,
					filters:  v3rpc.FiltersFromRequest(cr),
				}

				if !w.wr.valid() {
					w.post(&pb.WatchResponse{WatchId: -1, Created: true, Canceled: true})
					continue
				}

				wps.nextWatcherID++
				w.nextrev = cr.StartRevision
				wps.watchers[w.id] = w

				go func(wps *watchProxyStream, w *watcher) {
					wps.ranges.add(w)
				}(wps, w)
			}

		case *pb.WatchRequest_CancelRequest:
			wpsm.delete(uv.CancelRequest.WatchId)
		default:
			panic("not implemented")
		}
	}
}

func (wpsm *watchProxyStreamHub) sendLoop() {

	createChan := make(chan *pb.WatchResponse , wpsm.numClusters )

	defer close(createChan)

	counting := wpsm.numClusters

	for {
		select {
		case wresp, ok := <-wpsm.watchCh:

			if !ok {
				return
			}

			if wresp.Created {
				if counting == 0 {
					counting = wpsm.numClusters
				}

				if counting > 0 {

					plog.Infof("got create response %v, event %v", wresp, wresp.Events)
					counting--
					plog.Infof("%d more needed", counting)

					if counting != 0 {
						plog.Infof("dump create response %v, event %v", wresp, wresp.Events)
						continue // next response
					}

				}
			}

			if counting > 0 { // event response will not be ditched before all create events are confirmed
				continue
			}

			plog.Infof("send watch create response to client %v, event %v", wresp, wresp.Events)

			if err := wpsm.stream.Send(wresp); err != nil {
				return
			}
		case <-wpsm.ctx.Done():
			return
		}
	}
}

func (wpsm *watchProxyStreamHub) delete(id int64) {
	wpsm.mu.Lock()
	defer wpsm.mu.Unlock()

	for _, wps := range wpsm.watchProxyStreams {

		w, ok := wps.watchers[id]
		if !ok {
			return
		}
		wps.ranges.delete(w)
		delete(wps.watchers, id)
		resp := &pb.WatchResponse{
			Header:   &w.lastHeader,
			WatchId:  id,
			Canceled: true,
		}
		wpsm.watchCh <- resp
	}
}
