package services

import (
	"errors"
	"gp_upgrade/db"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"regexp"

	"github.com/cppforlife/go-semi-semantic/version"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jmoiron/sqlx"
	"golang.org/x/net/context"
)

func (h *HubClient) CheckVersion(ctx context.Context,
	in *pb.CheckVersionRequest) (*pb.CheckVersionReply, error) {

	gplog.Info("starting CheckVersion")
	dbConnector := db.NewDBConn(in.Host, int(in.DbPort), "template1")
	defer dbConnector.Close()
	err := dbConnector.Connect()
	if err != nil {
		gplog.Error(err.Error())
		return nil, utils.DatabaseConnectionError{Parent: err}
	}
	databaseHandler := dbConnector.GetConn()
	isVersionCompatible, err := VerifyVersion(databaseHandler)
	if err != nil {
		gplog.Error(err.Error())
		return nil, errors.New(err.Error())
	}
	return &pb.CheckVersionReply{IsVersionCompatible: isVersionCompatible}, nil
}

func VerifyVersion(dbHandler *sqlx.DB) (bool, error) {
	var row string
	err := dbHandler.Get(&row, VERSION_QUERY)
	if err != nil {
		gplog.Error(err.Error())
		return false, errors.New(err.Error())
	}
	re := regexp.MustCompile("Greenplum Database (.*) build")
	versionStringResults := re.FindStringSubmatch(row)
	if len(versionStringResults) < 2 {
		gplog.Error("didn't get a version string match")
		return false, errors.New("didn't get a version string match")
	}
	versionString := versionStringResults[1]
	versionObject, err := version.NewVersionFromString(versionString)
	if err != nil {
		gplog.Error(err.Error())
		return false, err
	}
	if versionObject.IsGt(version.MustNewVersionFromString(MINIMUM_VERSION)) {
		return true, nil
	}
	gplog.Error("falling through")
	return false, nil
}

const (
	VERSION_QUERY   = `SELECT version()`
	MINIMUM_VERSION = "4.3.9.0"
)
