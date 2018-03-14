package services

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/upgradestatus"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"os"
	"path/filepath"
)

var HomeDir = os.Getenv("HOME")

func (s *HubClient) StatusUpgrade(ctx context.Context, in *pb.StatusUpgradeRequest) (*pb.StatusUpgradeReply, error) {
	gplog.Info("starting StatusUpgrade")

	checkConfigStatePath := filepath.Join(HomeDir, ".gp_upgrade/cluster_config.json")
	gplog.Debug("looking for check-config.json at %s", checkConfigStatePath)

	checkconfigStatus := &pb.UpgradeStepStatus{
		Step: pb.UpgradeSteps_CHECK_CONFIG,
	}
	if _, err := utils.System.Stat(checkConfigStatePath); utils.System.IsNotExist(err) {
		checkconfigStatus.Status = pb.StepStatus_PENDING
	} else {
		checkconfigStatus.Status = pb.StepStatus_COMPLETE
	}

	prepareInitStatus, _ := GetPrepareNewClusterConfigStatus()

	seginstallStatePath := filepath.Join(HomeDir, ".gp_upgrade/seginstall")
	gplog.Debug("looking for seginstall State at %s", seginstallStatePath)
	seginstallState := upgradestatus.NewStateCheck(seginstallStatePath, pb.UpgradeSteps_SEGINSTALL)
	seginstallStatus, _ := seginstallState.GetStatus()

	gpstopStatePath := filepath.Join(HomeDir, ".gp_upgrade/gpstop")
	clusterPair := upgradestatus.NewShutDownClusters(gpstopStatePath)
	shutdownClustersStatus, _ := clusterPair.GetStatus()

	pgUpgradePath := filepath.Join(HomeDir, ".gp_upgrade/pg_upgrade")
	convertMaster := upgradestatus.NewConvertMaster(pgUpgradePath)
	masterUpgradeStatus, _ := convertMaster.GetStatus()

	startAgentsStatePath := filepath.Join(HomeDir, ".gp_upgrade/start-agents")
	prepareStartAgentsState := upgradestatus.NewStateCheck(startAgentsStatePath, pb.UpgradeSteps_PREPARE_START_AGENTS)
	startAgentsStatus, _ := prepareStartAgentsState.GetStatus()

	shareOidsPath := filepath.Join(HomeDir, ".gp_upgrade/share-oids")
	shareOidsState := upgradestatus.NewStateCheck(shareOidsPath, pb.UpgradeSteps_SHARE_OIDS)
	shareOidsStatus, _ := shareOidsState.GetStatus()

	return &pb.StatusUpgradeReply{
		ListOfUpgradeStepStatuses: []*pb.UpgradeStepStatus{
			checkconfigStatus,
			seginstallStatus,
			prepareInitStatus,
			shutdownClustersStatus,
			masterUpgradeStatus,
			startAgentsStatus,
			shareOidsStatus,
		},
	}, nil
}

func GetPrepareNewClusterConfigStatus() (*pb.UpgradeStepStatus, error) {
	/* Treat all stat failures as cannot find file. Conceal worse failures atm.*/
	_, err := utils.System.Stat(configutils.GetNewClusterConfigFilePath())

	if err != nil {
		gplog.Debug("%v", err)
		return &pb.UpgradeStepStatus{Step: pb.UpgradeSteps_PREPARE_INIT_CLUSTER,
			Status: pb.StepStatus_PENDING}, nil
	}

	return &pb.UpgradeStepStatus{Step: pb.UpgradeSteps_PREPARE_INIT_CLUSTER,
		Status: pb.StepStatus_COMPLETE}, nil
}
