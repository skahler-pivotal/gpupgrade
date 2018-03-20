package services

import (
	"os"
	"path/filepath"

	pb "gp_upgrade/idl"

	"gp_upgrade/hub/upgradestatus"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *HubClient) UpgradeShareOids(ctx context.Context, in *pb.UpgradeShareOidsRequest) (*pb.UpgradeShareOidsReply, error) {
	gplog.Info("Started processing share-oids request")
	go h.ShareOidFilesStub(h.conf.StateDir)
	return &pb.UpgradeShareOidsReply{}, nil
}

func (h *HubClient) ShareOidFilesStub(stateDir string) {
	c := upgradestatus.NewChecklistManager(stateDir)
	shareOidsStep := "share-oids"

	err := c.ResetStateDir(shareOidsStep)
	if err != nil {
		gplog.Error("error from ResetStateDir " + err.Error())
	}
	err = c.MarkInProgress(shareOidsStep)
	if err != nil {
		gplog.Error("error from MarkInProgress " + err.Error())
	}

	hostnames, err := h.configreader.GetHostnames()

	user := "gpadmin"
	rsyncFlags := "-rzpogt"
	sourceDir := filepath.Join(os.Getenv("HOME"), ".gp_upgrade", "pg_upgrade")
	gplog.Info("sourceDir" + sourceDir)

	if err == nil {
		anyFailed := false
		for _, host := range hostnames {
			destinationDirectory := user + "@" + host + ":" + filepath.Join(os.Getenv("HOME"), ".gp_upgrade")
			gplog.Info("destinationDirectory" + destinationDirectory)

			//output, err := utils.System.ExecCmdOutput("bash", "-c", "rsync", rsyncFlags, sourceDir, destinationDirectory)
			output, err := h.commandExecer("bash", "-c", "rsync", rsyncFlags, sourceDir, destinationDirectory).Output()

			if err != nil {
				gplog.Error(string(output))
				gplog.Error(err.Error())
				c.MarkFailed(shareOidsStep)
				anyFailed = true
			}
		}
		if !anyFailed {
			c.MarkComplete(shareOidsStep)
		}
	} else {
		gplog.Error("error from reading config" + err.Error())
	}
}
