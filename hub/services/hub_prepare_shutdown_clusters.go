package services

import (
	pb "gp_upgrade/idl"

	"golang.org/x/net/context"

	"path"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (h *HubClient) PrepareShutdownClusters(ctx context.Context, in *pb.PrepareShutdownClustersRequest) (*pb.PrepareShutdownClustersReply, error) {
	gplog.Info("starting PrepareShutdownClusters()")

	// will be initialized for future uses also? We think so -- it should
	initErr := h.clusterPair.Init(h.conf.StateDir, in.OldBinDir, in.NewBinDir, h.commandExecer)

	if initErr != nil {
		gplog.Error("An occurred during cluster pair init: %v", initErr)
		return nil, initErr
	}

	pathToGpstopStateDir := path.Join(h.conf.StateDir, "gpstop")
	go h.clusterPair.StopEverything(pathToGpstopStateDir)

	/* TODO: gpstop may take a while.
	 * How do we check if everything is stopped?
	 * Should we check bindirs for 'good-ness'? No...

	 * Use go routine along with using files as a way to keep track of gpstop state
	 */

	// XXX: May be tell user to run status, or if that seems stuck, check gpAdminLogs/gp_upgrade_hub*.log

	return &pb.PrepareShutdownClustersReply{}, nil
}
