package services

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/upgradestatus"

	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"gp_upgrade/hub/configutils"
	"strings"
)

const (
	port = "6416"
)

var DialTimeout = 3 * time.Second

type dialer func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

type reader interface {
	GetHostnames() ([]string, error)
	GetSegmentConfiguration() configutils.SegmentConfiguration
	OfOldClusterConfig()
}

type Connection struct {
	Conn     *grpc.ClientConn
	Hostname string
}

type HubClient struct {
	Bootstrapper

	agentConns   []*Connection
	clusterPair  cluster.PairOperator
	configreader reader
	grpcDialer   dialer
}

func NewHub(pair cluster.PairOperator, configReader reader, grpcDialer dialer) (*HubClient, func()) {
	// refactor opportunity -- don't use this pattern,
	// use different types or separate functions for old/new or set the config path at reader initialization time
	configReader.OfOldClusterConfig()
	gpUpgradeDir := filepath.Join(os.Getenv("HOME"), ".gp_upgrade")

	h := &HubClient{
		clusterPair:  pair,
		configreader: configReader,
		grpcDialer:   grpcDialer,
		Bootstrapper: Bootstrapper{
			hostnameGetter: configReader,
			remoteExecutor: NewClusterSsher(upgradestatus.NewChecklistManager(gpUpgradeDir), NewPingerManager()),
		},
	}

	return h, h.closeConns
}

func (h *HubClient) AgentConns() ([]*Connection, error) {
	if h.agentConns != nil {
		err := h.ensureConnsAreReady()
		if err != nil {
			return nil, err
		}

		return h.agentConns, nil
	}

	hostnames, err := h.configreader.GetHostnames()
	if err != nil {
		return nil, err
	}

	for _, host := range hostnames {
		ctx, _ := context.WithTimeout(context.TODO(), DialTimeout)
		conn, err := h.grpcDialer(ctx, host+":"+port, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		h.agentConns = append(h.agentConns, &Connection{
			Conn:     conn,
			Hostname: host,
		})
	}

	return h.agentConns, nil
}

func (h *HubClient) ensureConnsAreReady() error {
	var hostnames []string
	for i := 0; i < 3; i++ {
		ready := 0
		for _, conn := range h.agentConns {
			if conn.Conn.GetState() == connectivity.Ready {
				ready++
			} else {
				hostnames = append(hostnames, conn.Hostname)
			}
		}

		if ready == len(h.agentConns) {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("the connections to the following hosts were not ready: %s", strings.Join(hostnames, ","))
}

func (h *HubClient) closeConns() {
	for _, conn := range h.agentConns {
		err := conn.Conn.Close()
		if err != nil {
			gplog.Info(fmt.Sprintf("Error closing hub to agent connection. host: %s, err: %s", conn.Hostname, err.Error()))
		}
	}
}

func (h *HubClient) segmentsByHost() map[string]configutils.SegmentConfiguration {
	segments := h.configreader.GetSegmentConfiguration()

	segmentsByHost := make(map[string]configutils.SegmentConfiguration)
	for _, segment := range segments {
		host := segment.Hostname
		if len(segmentsByHost[host]) == 0 {
			segmentsByHost[host] = []configutils.Segment{segment}
		} else {
			segmentsByHost[host] = append(segmentsByHost[host], segment)
		}
	}

	return segmentsByHost
}
