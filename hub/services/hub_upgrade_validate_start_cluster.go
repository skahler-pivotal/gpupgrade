package services

import (
	pb "gp_upgrade/idl"

	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
	"gp_upgrade/hub/upgradestatus"
	"os"
)

func (h *HubClient) UpgradeValidateStartCluster(ctx context.Context,
	in *pb.UpgradeValidateStartClusterRequest) (*pb.UpgradeValidateStartClusterReply, error) {
	gplog.Info("Started processing validate-start-cluster request")

	err := h.startNewCluster(in.NewBinDir, in.NewDataDir)
	return &pb.UpgradeValidateStartClusterReply{}, err
}

func (h *HubClient) startNewCluster(newBinDir string, newDataDir string) error {
	gplog.Error(h.conf.StateDir)
	c := upgradestatus.NewChecklistManager(h.conf.StateDir)
	err := c.ResetStateDir("validate-start-cluster")
	if err != nil {
		gplog.Error("failed to reset the state dir for validate-start-cluster")

		return err
	}

	err = c.MarkInProgress("validate-start-cluster")
	if err != nil {
		gplog.Error("failed to record in-progress for validate-start-cluster")

		return err
	}

	commandArgs := fmt.Sprintf("PYTHONPATH=%s && %s/gpstart -a -d %s", os.Getenv("PYTHONPATH"), newBinDir, newDataDir)
	_, err = h.commandExecer("bash", "-c", commandArgs).Output()
	if err != nil {
		gplog.Error(err.Error())
		cmErr := c.MarkFailed("validate-start-cluster")
		if cmErr != nil {
			gplog.Error("failed to record failed for validate-start-cluster")
		}

		return err
	}

	err = c.MarkComplete("validate-start-cluster")
	if err != nil {
		gplog.Error("failed to record completed for validate-start-cluster")
		return err
	}

	return nil
}
