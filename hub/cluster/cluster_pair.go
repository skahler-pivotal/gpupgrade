package cluster

import (
	"gp_upgrade/hub/configutils"
	"gp_upgrade/utils"

	"fmt"
	"gp_upgrade/hub/upgradestatus"
	"os/exec"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type PairOperator interface {
	Init(string, string) error
	StopEverything(string)
}

type Pair struct {
	upgradeConfig          configutils.UpgradeConfig
	oldMasterPort          int
	newMasterPort          int
	oldMasterDataDirectory string
	newMasterDataDirectory string
	oldBinDir              string
	newBinDir              string
}

func (cp *Pair) Init(oldBinDir string, newBinDir string) error {
	var err error
	cp.oldBinDir = oldBinDir
	cp.newBinDir = newBinDir

	cp.upgradeConfig, err = configutils.GetUpgradeConfig()
	if err != nil {
		return fmt.Errorf("couldn't read config files: %v", err)
	}

	cp.oldMasterPort, cp.newMasterPort, err = cp.upgradeConfig.GetMasterPorts()
	if err != nil {
		return err
	}

	cp.oldMasterDataDirectory, cp.newMasterDataDirectory, err = cp.upgradeConfig.GetMasterDataDirs()
	if err != nil {
		return err
	}

	return nil
}

func (cp *Pair) StopEverything(pathToGpstopStateDir string) {
	checklistManager := upgradestatus.NewChecklistManager(pathToGpstopStateDir)
	checklistManager.ResetStateDir("gpstop.old")
	checklistManager.ResetStateDir("gpstop.new")

	oldGpstopShellArgs := fmt.Sprintf("PGPORT=%d && MASTER_DATA_DIRECTORY=%s && %s/gpstop -a",
		cp.oldMasterPort, cp.oldMasterDataDirectory, cp.oldBinDir)
	runOldStopCmd := utils.System.ExecCommand("bash", "-c", oldGpstopShellArgs)

	newGpstopShellArgs := fmt.Sprintf("PGPORT=%d && MASTER_DATA_DIRECTORY=%s && %s/gpstop -a", cp.newMasterPort,
		cp.newMasterDataDirectory, cp.newBinDir)
	runNewStopCmd := utils.System.ExecCommand("bash", "-c", newGpstopShellArgs)

	stopCluster(runOldStopCmd, "gpstop.old", checklistManager)
	stopCluster(runNewStopCmd, "gpstop.new", checklistManager)
}

func stopCluster(stopCmd *exec.Cmd, baseName string, stateManager *upgradestatus.ChecklistManager) {
	err := stateManager.MarkInProgress(baseName)
	if err != nil {
		gplog.Error(err.Error())
		return
	}

	err = stopCmd.Run()

	gplog.Info("finished stopping %s", baseName)

	if err != nil {
		gplog.Error(err.Error())
		stateManager.MarkFailed(baseName)
		return
	}
	stateManager.MarkComplete(baseName)
}
