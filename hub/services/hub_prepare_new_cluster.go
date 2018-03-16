package services

import (
	"gp_upgrade/db"
	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *HubClient) PrepareInitCluster(ctx context.Context,
	in *pb.PrepareInitClusterRequest) (*pb.PrepareInitClusterReply, error) {

	gplog.Info("starting PrepareInitCluster()")
	dbConnector := db.NewDBConn("localhost", int(in.DbPort), "template1")
	defer dbConnector.Close()
	err := dbConnector.Connect()
	if err != nil {
		gplog.Error(err.Error())
		return nil, utils.DatabaseConnectionError{Parent: err}
	}
	databaseHandler := dbConnector.GetConn()

	configQuery := `select dbid, content, role, preferred_role,
		mode, status, port, hostname, address, datadir
		from gp_segment_configuration`
	err = SaveQueryResultToJSON(databaseHandler, configQuery,
		configutils.NewWriter(h.conf.StateDir, configutils.GetNewClusterConfigFilePath(h.conf.StateDir)))
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}
	return &pb.PrepareInitClusterReply{}, nil
}
