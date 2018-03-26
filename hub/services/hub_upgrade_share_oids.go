package services

import (
	"path/filepath"

	pb "gp_upgrade/idl"

	"gp_upgrade/hub/upgradestatus"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *HubClient) UpgradeShareOids(ctx context.Context, in *pb.UpgradeShareOidsRequest) (*pb.UpgradeShareOidsReply, error) {
	gplog.Info("Started processing share-oids request")
	go h.ShareOidFilesStub()
	return &pb.UpgradeShareOidsReply{}, nil
}

func (h *HubClient) ShareOidFilesStub() {
	c := upgradestatus.NewChecklistManager(h.conf.StateDir)
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
	sourceDir := filepath.Join(h.conf.StateDir, "pg_upgrade")
	gplog.Info("sourceDir" + sourceDir)

	if err == nil {
		anyFailed := false
		for _, host := range hostnames {
			destinationDirectory := user + "@" + host + ":" + h.conf.StateDir
			gplog.Info("destinationDirectory" + destinationDirectory)

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
