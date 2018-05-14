package services

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/hub/configutils"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func SaveTargetClusterConfig(dbConnector *dbconn.DBConn, stateDir string, newBinDir string) error {
	segConfig := make(configutils.SegmentConfiguration, 0)
	var configQuery string

	configQuery = CONFIGQUERY6
	if dbConnector.Version.Before("6") {
		configQuery = CONFIGQUERY5
	}

	configFile, err := operating.System.OpenFileWrite(configutils.GetNewClusterConfigFilePath(stateDir), os.O_CREATE|os.O_WRONLY, 0700)
	if err != nil {
		errMsg := fmt.Sprintf("Could not open new config file for writing. Err: %s", err.Error())
		return errors.New(errMsg)
	}

	err = dbConnector.Select(&segConfig, configQuery)
	if err != nil {
		errMsg := fmt.Sprintf("Unable to execute query %s. Err: %s", configQuery, err.Error())
		return errors.New(errMsg)
	}

	clusterConfig := configutils.ClusterConfig{
		SegConfig: segConfig,
		BinDir:    newBinDir,
	}

	err = SaveQueryResultToJSON(&clusterConfig, configFile)
	if err != nil {
		return err
	}

	return nil
}

func GetOpenPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func (h *HubClient) getNewMasterPort(port int) (int, error) {
	masterPort := port + 1
	cmdStr := fmt.Sprintf(`netstat -n | awk '{ print $4 }' | grep -o "\.%d*$"`, masterPort)
	findPortCmd := h.commandExecer("bash", "-c", cmdStr)
	output, err := findPortCmd.Output()
	if err == nil && string(output) == "" {
		return masterPort, nil
	}
	masterPort, err = GetOpenPort()
	return masterPort, err
}

func CheckAllFreePorts(agentConns []*Connection, possiblePortBase int, numPrimaries int) bool {
	wg := sync.WaitGroup{}
	agentErrs := make(chan error, len(agentConns))
	for _, agentConn := range agentConns {
		wg.Add(1)
		go func(c *Connection) {
			defer wg.Done()

			_, err := c.PbAgentClient.CheckFreePorts(context.Background(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			})

			if err != nil {
				gplog.Error("failed to find free ports: %v", err)
				agentErrs <- err
			}
		}(agentConn)
	}

	wg.Wait()

	if len(agentErrs) != 0 {
		return false
	}

	return true
}

type CheckAllFreePortsFunc func([]*Connection, int, int) bool

func GetFreePortBase(oldReader reader, checkAllFreePorts CheckAllFreePortsFunc, agentConns []*Connection) (int, error) {
	numPrimaries := len(oldReader.GetSegmentConfiguration()) - 1
	possiblePortBase := oldReader.GetMaxSegmentPort() + 1
	maxTries := 10
	for i := 0; i < maxTries; i++ {
		allPortsFree := checkAllFreePorts(agentConns, possiblePortBase, numPrimaries)
		if allPortsFree {
			return possiblePortBase, nil
		}
		possiblePortBase = rand.Intn(math.MaxInt32-1024) + 1024
	}
	return 0, errors.New("no free port base found")
}

func (h *Hub) PrepareInitCluster(ctx context.Context, in *pb.PrepareInitClusterRequest) (*pb.PrepareInitClusterReply, error) {
	gplog.Info("starting PrepareInitCluster()")

	// Read original cluster config file
	oldReader := configutils.NewReader()
	oldReader.OfOldClusterConfig(h.conf.StateDir)

	// Create gpinitsystem_config file for the new cluster from the old config
	gpinitsystemConfig := []string{`ARRAY_NAME="gp_upgrade cluster"`}

	//seg prefix
	segPrefix := path.Base(oldReader.GetMasterDataDir())
	segPrefix = segPrefix[:len(segPrefix)-2]
	gpinitsystemConfig = append(gpinitsystemConfig, "SEG_PREFIX="+segPrefix)

	//determine good chunk of port base, try old port base +1 first
	agentConns, err := h.AgentConns()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	portBase, err := GetFreePortBase(&oldReader, CheckAllFreePorts, agentConns)
	if err != nil {
		return &pb.PrepareInitClusterReply{}, errors.New("did not find a free range of ports")
	}

	gpinitsystemConfig = append(gpinitsystemConfig, fmt.Sprintf("PORT_BASE=%d", portBase))

	//set datadirs
	var datadirs []string
	for _, datadir := range oldReader.GetSegmentDataDirs() {
		dir := fmt.Sprintf("%s_upgrade", path.Dir(datadir))
		datadirs = append(datadirs, dir)
	}

	datadirDeclare := fmt.Sprintf("declare -a DATA_DIRECTORY=(%s)", strings.Join(datadirs, " "))
	gpinitsystemConfig = append(gpinitsystemConfig, datadirDeclare)

	//set master hostname
	hostname, err := os.Hostname()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	gpinitsystemConfig = append(gpinitsystemConfig, fmt.Sprintf("MASTER_HOSTNAME=%s", hostname))

	//set master datadir
	masterDataDir := fmt.Sprintf("MASTER_DIRECTORY=%s_upgrade", oldReader.GetMasterDataDir())
	gpinitsystemConfig = append(gpinitsystemConfig, masterDataDir)

	//set master port
	masterPort, err := h.getNewMasterPort(oldReader.GetPortForSegment(-1))
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}
	gpinitsystemConfig = append(gpinitsystemConfig, string(masterPort))

	gpinitsystemConfig = append(gpinitsystemConfig, "TRUSTED_SHELL=ssh")
	gpinitsystemConfig = append(gpinitsystemConfig, "CHECK_POINT_SEGMENTS=8")
	gpinitsystemConfig = append(gpinitsystemConfig, "ENCODING=UNICODE")

	gpinitsystemContents := []byte(strings.Join(gpinitsystemConfig, "\n"))
	gpinitsystemFilepath := filepath.Join(h.conf.StateDir, "gpinitsystem_config")
	err = ioutil.WriteFile(gpinitsystemFilepath, gpinitsystemContents, 0644)

	//write the hostnameFile
	hostnames, err := oldReader.GetHostnames()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}
	hostnameFilepath := filepath.Join(h.conf.StateDir, "hostfile")
	err = ioutil.WriteFile(hostnameFilepath, []byte(strings.Join(hostnames, "\n")), 0644)

	// gpinitsystem the new cluster
	cmdStr := fmt.Sprintf("gpinitsystem -c %s -h %s", gpinitsystemFilepath, hostnameFilepath)
	gpinitsystemCmd := h.commandExecer("bash", "-c", cmdStr)
	_, err = gpinitsystemCmd.Output()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	dbConnector := db.NewDBConn("localhost", int(in.DbPort), "template1")
	defer dbConnector.Close()
	err = dbConnector.Connect(1)
	if err != nil {
		gplog.Error(err.Error())
		return &pb.PrepareInitClusterReply{}, utils.DatabaseConnectionError{Parent: err}
	}
	dbConnector.Version.Initialize(dbConnector)

	err = SaveTargetClusterConfig(dbConnector, h.conf.StateDir, in.NewBinDir)
	if err != nil {
		gplog.Error(err.Error())
		return &pb.PrepareInitClusterReply{}, err
	}

	return &pb.PrepareInitClusterReply{}, nil
}
