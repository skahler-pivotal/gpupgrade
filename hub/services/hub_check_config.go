package services

import (
	"gp_upgrade/db"
	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func (h *HubClient) CheckConfig(ctx context.Context,
	in *pb.CheckConfigRequest) (*pb.CheckConfigReply, error) {

	gplog.Info("starting CheckConfig()")
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
		configutils.NewWriter(h.conf.StateDir, configutils.GetConfigFilePath(h.conf.StateDir)))
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}

	versionQuery := `show gp_server_version_num`
	err = SaveQueryResultToJSON(databaseHandler, versionQuery,
		configutils.NewWriter(h.conf.StateDir, configutils.GetVersionFilePath(h.conf.StateDir)))
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}

	successReply := &pb.CheckConfigReply{ConfigStatus: "All good"}
	return successReply, nil
}

// public for testing purposes
func SaveQueryResultToJSON(databaseHandler *sqlx.DB, configQuery string, writer configutils.Store) error {
	rows, err := databaseHandler.Query(configQuery)
	if err != nil {
		gplog.Error(err.Error())
		return errors.New(err.Error())
	}
	defer rows.Close()

	err = writer.Load(rows)
	if err != nil {
		gplog.Error(err.Error())
		return errors.New(err.Error())
	}

	err = writer.Write()
	if err != nil {
		gplog.Error(err.Error())
		return errors.New(err.Error())
	}

	return nil
}
