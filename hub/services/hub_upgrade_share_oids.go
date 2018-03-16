package services

import (
	pb "gp_upgrade/idl"

	"gp_upgrade/hub/upgradestatus"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *HubClient) UpgradeShareOids(ctx context.Context, in *pb.UpgradeShareOidsRequest) (*pb.UpgradeShareOidsReply, error) {
	gplog.Info("Started processing share-oids request")
	shareOidFilesStub(h.conf.StateDir)
	return &pb.UpgradeShareOidsReply{}, nil
}

func shareOidFilesStub(stateDir string) {
	c := upgradestatus.NewChecklistManager(stateDir)
	c.ResetStateDir("share-oids")
	c.MarkInProgress("share-oids")
	c.MarkComplete("share-oids")
}
