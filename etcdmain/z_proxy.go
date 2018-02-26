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

package etcdmain

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/leasing"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/clientv3/ordering"
	"github.com/coreos/etcd/etcdserver/api/etcdhttp"
	"github.com/coreos/etcd/etcdserver/api/v3election/v3electionpb"
	"github.com/coreos/etcd/etcdserver/api/v3lock/v3lockpb"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/proxy/grpcproxy"

	"github.com/coreos/pkg/capnslog"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"encoding/json"
	"strings"
)

func init() {
	rootCmd.AddCommand(newZProxyCommand())
}

// newGRPCProxyCommand returns the cobra command for "grpc-proxy".
func newZProxyCommand() *cobra.Command {
	lpc := &cobra.Command{
		Use:   "z-proxy <subcommand>",
		Short: "z-proxy related command",
	}
	lpc.AddCommand(newZProxyStartCommand())

	return lpc
}

func newZProxyStartCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "start",
		Short: "start the z-proxy",
		Run:   startZProxy,
	}

	cmd.Flags().StringVar(&grpcProxyListenAddr, "listen-addr", "127.0.0.1:23790", "listen address")
	cmd.Flags().StringVar(&grpcProxyDNSCluster, "discovery-srv", "", "DNS domain used to bootstrap initial cluster")
	cmd.Flags().StringVar(&grpcProxyMetricsListenAddr, "metrics-addr", "", "listen for /metrics requests on an additional interface")
	cmd.Flags().BoolVar(&grpcProxyInsecureDiscovery, "insecure-discovery", false, "accept insecure SRV records")
	cmd.Flags().StringSliceVar(&grpcProxyEndpoints, "endpoints", []string{"127.0.0.1:2379"}, "comma separated etcd cluster endpoints")
	cmd.Flags().StringVar(&grpcProxyAdvertiseClientURL, "advertise-client-url", "127.0.0.1:23790", "advertise address to register (must be reachable by client)")
	cmd.Flags().StringVar(&grpcProxyResolverPrefix, "resolver-prefix", "", "prefix to use for registering proxy (must be shared with other grpc-proxy members)")
	cmd.Flags().IntVar(&grpcProxyResolverTTL, "resolver-ttl", 0, "specify TTL, in seconds, when registering proxy endpoints")
	cmd.Flags().StringVar(&grpcProxyNamespace, "namespace", "", "string to prefix to all keys for namespacing requests")
	cmd.Flags().BoolVar(&grpcProxyEnablePprof, "enable-pprof", false, `Enable runtime profiling data via HTTP server. Address is at client URL + "/debug/pprof/"`)
	cmd.Flags().StringVar(&grpcProxyDataDir, "data-dir", "default.proxy", "Data directory for persistent data")
	cmd.Flags().IntVar(&grpcMaxCallSendMsgSize, "max-send-bytes", defaultGRPCMaxCallSendMsgSize, "message send limits in bytes (default value is 1.5 MiB)")
	cmd.Flags().IntVar(&grpcMaxCallRecvMsgSize, "max-recv-bytes", math.MaxInt32, "message receive limits in bytes (default value is math.MaxInt32)")

	// client TLS for connecting to server
	cmd.Flags().StringVar(&grpcProxyCert, "cert", "", "identify secure connections with etcd servers using this TLS certificate file")
	cmd.Flags().StringVar(&grpcProxyKey, "key", "", "identify secure connections with etcd servers using this TLS key file")
	cmd.Flags().StringVar(&grpcProxyCA, "cacert", "", "verify certificates of TLS-enabled secure etcd servers using this CA bundle")
	cmd.Flags().BoolVar(&grpcProxyInsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "skip authentication of etcd server TLS certificates")

	// client TLS for connecting to proxy
	cmd.Flags().StringVar(&grpcProxyListenCert, "cert-file", "", "identify secure connections to the proxy using this TLS certificate file")
	cmd.Flags().StringVar(&grpcProxyListenKey, "key-file", "", "identify secure connections to the proxy using this TLS key file")
	cmd.Flags().StringVar(&grpcProxyListenCA, "trusted-ca-file", "", "verify certificates of TLS-enabled secure proxy using this CA bundle")
	cmd.Flags().BoolVar(&grpcProxyListenAutoTLS, "auto-tls", false, "proxy TLS using generated certificates")
	cmd.Flags().StringVar(&grpcProxyListenCRL, "client-crl-file", "", "proxy client certificate revocation list file.")

	// experimental flags
	cmd.Flags().BoolVar(&grpcProxyEnableOrdering, "experimental-serializable-ordering", false, "Ensure serializable reads have monotonically increasing store revisions across endpoints.")
	cmd.Flags().StringVar(&grpcProxyLeasing, "experimental-leasing-prefix", "", "leasing metadata prefix for disconnected linearized reads.")

	cmd.Flags().BoolVar(&grpcProxyDebug, "debug", false, "Enable debug-level logging for grpc-proxy.")

	return &cmd
}


func startZProxy(cmd *cobra.Command, args []string) {
	checkArgs()

	capnslog.SetGlobalLogLevel(capnslog.INFO)
	if grpcProxyDebug {
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
		grpc.EnableTracing = true
		// enable info, warning, error
		grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))
	} else {
		// only discard info
		grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, os.Stderr, os.Stderr))
	}

	tlsinfo := newTLS(grpcProxyListenCA, grpcProxyListenCert, grpcProxyListenKey)
	if tlsinfo == nil && grpcProxyListenAutoTLS {
		host := []string{"https://" + grpcProxyListenAddr}
		dir := filepath.Join(grpcProxyDataDir, "fixtures", "proxy")
		autoTLS, err := transport.SelfCert(dir, host)
		if err != nil {
			plog.Fatal(err)
		}
		tlsinfo = &autoTLS
	}
	if tlsinfo != nil {
		plog.Infof("ServerTLS: %s", tlsinfo)
	}
	m := mustListenCMux(tlsinfo)

	grpcl := m.Match(cmux.HTTP2())
	defer func() {
		grpcl.Close()
		plog.Infof("stopping listening for grpc-proxy client requests on %s", grpcProxyListenAddr)
	}()

	client := mustNewClient()

	bootstrapResp, bootstrapError := client.KV.Get(context.TODO(), "/bootstrap")

	if  bootstrapError != nil || bootstrapResp == nil || len(bootstrapResp.Kvs) == 0{
		fatal := "Fail to load bootstrap, Z-proxy cannot start."
		plog.Fatal(fatal)
		panic(fatal)
	}

	rawBootstrapData := bootstrapResp.Kvs[0].Value

	/*
		boostrap example
	{
		\"/_GD6\":[\"10.199.206.73:2379\",\"10.199.206.73:12379\",\"10.199.206.73:22379\"],
		\"/_GD9\":[\"10.199.206.72:2379\", \"10.199.206.72:12379\",\"10.199.206.72:22379\"]
	}
	*/

	bootstrapData := make(map[string][]string)

	plog.Infof("bootstrap data is %s", string(rawBootstrapData))

	json.Unmarshal(rawBootstrapData, &bootstrapData)

	plog.Infof("%d cluster(s) found",  len(bootstrapData))

	readerClients := make([]*clientv3.Client, len(bootstrapData))

	index := 0

	for zone, endpoints := range bootstrapData {
		// create remote clients if it is not local

		if strings.Compare(zone, grpcProxyNamespace ) == 0 {
			plog.Infof("use bootstrap client %s", zone)
			readerClients[index] = client
		} else {
			plog.Infof("create client for %s", zone)
			readerClient := mustRemoteClient(endpoints)
			readerClient.KV = namespace.NewKV(readerClient.KV, zone)
			readerClient.Watcher = namespace.NewWatcher(readerClient.Watcher, zone)
			readerClients[index] = readerClient
		}

		index++
	}

	srvhttp, httpl := mustHTTPListener(m, tlsinfo, client)
	errc := make(chan error)
	go func() { errc <- newZProxyServer(client, &readerClients).Serve(grpcl) }()
	go func() { errc <- srvhttp.Serve(httpl) }()
	go func() { errc <- m.Serve() }()
	if len(grpcProxyMetricsListenAddr) > 0 {
		mhttpl := mustMetricsListener(tlsinfo)
		go func() {
			mux := http.NewServeMux()
			etcdhttp.HandlePrometheus(mux)
			grpcproxy.HandleHealth(mux, client)
			plog.Fatal(http.Serve(mhttpl, mux))
		}()
	}

	// grpc-proxy is initialized, ready to serve
	notifySystemd()

	fmt.Fprintln(os.Stderr, <-errc)
	os.Exit(1)
}

func mustRemoteClient(remoteEndpoints []string) *clientv3.Client {

	srvs := discoverEndpoints(grpcProxyDNSCluster, grpcProxyCA, grpcProxyInsecureDiscovery)
	eps := srvs.Endpoints
	if len(eps) == 0 {
		eps = remoteEndpoints
	}
	cfg, err := newClientCfg(eps)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg.DialOptions = append(cfg.DialOptions,
		grpc.WithUnaryInterceptor(grpcproxy.AuthUnaryClientInterceptor))
	cfg.DialOptions = append(cfg.DialOptions,
		grpc.WithStreamInterceptor(grpcproxy.AuthStreamClientInterceptor))
	client, err := clientv3.New(*cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return client

}


func newZProxyServer(client *clientv3.Client,   readerClients *[]*clientv3.Client) *grpc.Server {
	if grpcProxyEnableOrdering {
		vf := ordering.NewOrderViolationSwitchEndpointClosure(*client)
		client.KV = ordering.NewKV(client.KV, vf)
		plog.Infof("waiting for linearized read from cluster to recover ordering")
		for {
			_, err := client.KV.Get(context.TODO(), "_", clientv3.WithKeysOnly())
			if err == nil {
				break
			}
			plog.Warningf("ordering recovery failed, retrying in 1s (%v)", err)
			time.Sleep(time.Second)
		}
	}

	if len(grpcProxyNamespace) > 0 {
		client.KV = namespace.NewKV(client.KV, grpcProxyNamespace)
		client.Watcher = namespace.NewWatcher(client.Watcher, grpcProxyNamespace)
		client.Lease = namespace.NewLease(client.Lease, grpcProxyNamespace)
	}

	if len(grpcProxyLeasing) > 0 {
		client.KV, _, _ = leasing.NewKV(client, grpcProxyLeasing)
	}

	kvp, _ := grpcproxy.NewZKvProxy(client, readerClients)
	//watchp, _ := grpcproxy.NewZWatchProxy(client)
	if grpcProxyResolverPrefix != "" {
		grpcproxy.Register(client, grpcProxyResolverPrefix, grpcProxyAdvertiseClientURL, grpcProxyResolverTTL)
	}
	clusterp, _ := grpcproxy.NewClusterProxy(client, grpcProxyAdvertiseClientURL, grpcProxyResolverPrefix)
	leasep, _ := grpcproxy.NewLeaseProxy(client)
	mainp := grpcproxy.NewMaintenanceProxy(client)
	authp := grpcproxy.NewAuthProxy(client)
	electionp := grpcproxy.NewElectionProxy(client)
	lockp := grpcproxy.NewLockProxy(client)

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.MaxConcurrentStreams(math.MaxUint32),
	)

	pb.RegisterKVServer(server, kvp)
	//pb.RegisterWatchServer(server, watchp)
	pb.RegisterClusterServer(server, clusterp)
	pb.RegisterLeaseServer(server, leasep)
	pb.RegisterMaintenanceServer(server, mainp)
	pb.RegisterAuthServer(server, authp)
	v3electionpb.RegisterElectionServer(server, electionp)
	v3lockpb.RegisterLockServer(server, lockp)

	// set zero values for metrics registered for this grpc server
	grpc_prometheus.Register(server)

	return server
}
