package commanders

import (
	"context"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type ConfigChecker struct {
	client pb.CliToHubClient
}

func NewConfigChecker(client pb.CliToHubClient) ConfigChecker {
	return ConfigChecker{
		client: client,
	}
}

func (req ConfigChecker) Execute() error {
	_, err := req.client.CheckConfig(context.Background(),
		&pb.CheckConfigRequest{})
	if err != nil {
		gplog.Error("ERROR - gRPC call to hub failed")
		return err
	}
	gplog.Info("Check config request is processed.")
	return nil
}
