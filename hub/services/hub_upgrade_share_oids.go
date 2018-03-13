package services

import (
	pb "gp_upgrade/idl"

	"gp_upgrade/hub/upgradestatus"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (s *HubClient) UpgradeShareOids(ctx context.Context,
	in *pb.UpgradeShareOidsRequest) (*pb.UpgradeShareOidsReply, error) {
	gplog.Info("Started processing share-oids request")
	shareOidFilesStub()
	return &pb.UpgradeShareOidsReply{}, nil
}

func shareOidFilesStub() {
	gpUpgradeDir := filepath.Join(os.Getenv("HOME"), ".gp_upgrade")
	c := upgradestatus.NewChecklistManager(gpUpgradeDir)
	c.ResetStateDir("share-oids")
	c.MarkInProgress("share-oids")
	c.MarkComplete("share-oids")
}
