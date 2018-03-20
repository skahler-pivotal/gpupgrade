package commanders

import (
	"context"

	pb "gp_upgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Upgrader struct {
	client pb.CliToHubClient
}

func NewUpgrader(client pb.CliToHubClient) Upgrader {
	return Upgrader{client: client}
}

func (u Upgrader) ConvertMaster(oldDataDir string, oldBinDir string, newDataDir string, newBinDir string) error {
	upgradeConvertMasterRequest := pb.UpgradeConvertMasterRequest{
		OldDataDir: oldDataDir,
		OldBinDir:  oldBinDir,
		NewDataDir: newDataDir,
		NewBinDir:  newBinDir,
	}
	_, err := u.client.UpgradeConvertMaster(context.Background(), &upgradeConvertMasterRequest)
	if err != nil {
		// TODO: Change the logging message?
		gplog.Error("ERROR - Unable to connect to hub")
		return err
	}

	gplog.Info("Kicked off pg_upgrade request.")
	return nil
}

func (u Upgrader) ShareOids() error {
	_, err := u.client.UpgradeShareOids(context.Background(), &pb.UpgradeShareOidsRequest{})
	if err != nil {
		gplog.Error(err.Error())
		return err
	}
	return nil
}

func (u Upgrader) ValidateStartCluster(newDataDir string, newBinDir string) error {
	_, err := u.client.UpgradeValidateStartCluster(context.Background(), &pb.UpgradeValidateStartClusterRequest{
		NewDataDir: newDataDir,
		NewBinDir:  newBinDir,
	})
	if err != nil {
		gplog.Error(err.Error())
		return err
	}
	return nil
}
