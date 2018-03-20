package services

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

var (
	GetMasterDataDirs = getMasterDataDirs
)

func (h *HubClient) UpgradeConvertMaster(ctx context.Context, in *pb.UpgradeConvertMasterRequest) (*pb.UpgradeConvertMasterReply, error) {
	gplog.Info("Starting master upgrade")
	//need to remember where we ran, i.e. pathToUpgradeWD, b/c pg_upgrade generates some files that need to be copied to QE nodes later
	//this is also where the 1.done, 2.inprogress ... files will be written
	err := h.ConvertMaster(h.conf.StateDir, "pg_upgrade", in.OldBinDir, in.NewBinDir)
	if err != nil {
		gplog.Error("%v", err)
		return nil, err
	}

	return &pb.UpgradeConvertMasterReply{}, nil
}

func (h *HubClient) ConvertMaster(baseDir, upgradeFileName, oldBinDir, newBinDir string) error {
	pathToUpgradeWD := filepath.Join(baseDir, upgradeFileName)
	err := os.Mkdir(pathToUpgradeWD, 0700)
	if err != nil {
		gplog.Error("mkdir %s failed: %v. Is there an pg_upgrade in progress?", pathToUpgradeWD, err)
	}

	pgUpgradeLog := filepath.Join(pathToUpgradeWD, "/pg_upgrade_master.log")
	f, _ := os.Create(pgUpgradeLog) /* We already made sure above that we have a prestine directory */

	oldMasterDataDir, newMasterDataDir, err := GetMasterDataDirs(baseDir)
	if err != nil {
		return err
	}

	upgradeCmdArgs := fmt.Sprintf("unset PGHOST; unset PGPORT; cd %s && nohup %s --old-bindir=%s --old-datadir=%s --new-bindir=%s --new-datadir=%s --dispatcher-mode --progress",
		pathToUpgradeWD, newBinDir+"/pg_upgrade", oldBinDir, oldMasterDataDir, newBinDir, newMasterDataDir)

	//export ENV VARS instead of passing on cmd line?
	upgradeCommand := h.commandExecer("bash", "-c", upgradeCmdArgs)
	cmd, ok := upgradeCommand.(*exec.Cmd)
	if ok {
		cmd.Stdout = f
		cmd.Stderr = f
	}

	//TODO check the rc on this? keep a pid?
	err = upgradeCommand.Start()
	if err != nil {
		gplog.Error("An error occured: %v", err)
		return err
	}

	gplog.Info("Upgrade command: %v", upgradeCommand)
	gplog.Info("Found no errors when starting the upgrade")

	return nil
}

func getMasterDataDirs(baseDir string) (string, string, error) {
	var err error
	reader := configutils.Reader{}
	reader.OfOldClusterConfig(baseDir)
	err = reader.Read()
	if err != nil {
		gplog.Error("Unable to read the file: %v", err)
		return "", "", err
	}

	oldMasterDataDir := reader.GetMasterDataDir()
	if oldMasterDataDir == "" {
		return "", "", errors.New("could not find old master data directory")
	}

	reader = configutils.Reader{}
	reader.OfNewClusterConfig(baseDir)
	err = reader.Read()
	if err != nil {
		gplog.Error("Unable to read the file: %v", err)
		return "", "", err
	}

	newMasterDataDir := reader.GetMasterDataDir()
	if oldMasterDataDir == "" {
		return "", "", errors.New("could not find old master data directory")
	}

	return oldMasterDataDir, newMasterDataDir, err
}
