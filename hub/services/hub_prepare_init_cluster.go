package services

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"

	"sync"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/helpers"
	"github.com/greenplum-db/gpupgrade/hub/configutils"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

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
	addr, err := utils.System.ResolveTCPAddr("tcp", "localhost:0")
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

func GetNewMasterPort(commandExecer helpers.CommandExecer, port int) (int, error) {
	newMasterPort := port + 1
	// If the following command returns 0, the port is not in the output and so is free for use.
	cmdStr := fmt.Sprintf(`netstat -n | grep -v -o "\.%d*$"`,
		newMasterPort)
	findPortCmd := commandExecer("bash", "-c", cmdStr)
	_, err := findPortCmd.Output()
	if err == nil {
		return newMasterPort, nil
	}
	newMasterPort, err = GetOpenPort()
	if err != nil {
		return 0, err
	}
	return newMasterPort, nil
}

func CheckAllFreePorts(agentConns []*Connection, possiblePortBase int, numPrimaries int) bool {
	wg := sync.WaitGroup{}
	freeChan := make(chan bool, len(agentConns))
	for _, agentConn := range agentConns {
		wg.Add(1)
		go func(c *Connection) {
			defer wg.Done()

			freeRangeFound, err := c.PbAgentClient.CheckFreePorts(context.Background(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			})

			if err != nil {
				gplog.Error("Error checking ports: %s", err.Error())
			}
			freeChan <- freeRangeFound.GetResult() && err == nil

		}(agentConn)
	}

	wg.Wait()
	close(freeChan)

	for b := range freeChan {
		if !b {
			return false
		}
	}
	return true
}

type CheckAllFreePortsFunc func([]*Connection, int, int) bool

func GetFreePortBase(oldReader reader, checkAllFreePorts CheckAllFreePortsFunc, agentConns []*Connection) (int, error) {
	numPrimaries := len(oldReader.GetSegmentConfiguration()) - 1
	possiblePortBase := oldReader.GetMaxSegmentPort() + 1
	maxTries := 10
	for i := 0; i < maxTries; i++ {
		gplog.Info("Checking if port %d is free", possiblePortBase)
		allPortsFree := checkAllFreePorts(agentConns, possiblePortBase, numPrimaries)
		if allPortsFree {
			gplog.Info("Found free port %d", possiblePortBase)
			return possiblePortBase, nil
		}
		possiblePortBase = rand.Intn(100000-1024) + 1024
	}
	gplog.Info("Did not find any free port after %d tries", maxTries)
	return 0, errors.New("no free port base found")
}

func CreateSegmentDataDirectories(agentConns []*Connection, datadirs []string) error {
	wg := sync.WaitGroup{}
	errChan := make(chan error, len(agentConns))
	for _, agentConn := range agentConns {
		wg.Add(1)
		go func(c *Connection) {
			defer wg.Done()

			_, err := c.PbAgentClient.CreateSegmentDataDirectories(context.Background(), &pb.CreateSegmentDataDirRequest{
				Datadirs: datadirs,
			})

			if err != nil {
				gplog.Error("Error creating segment data directories on host %s: %s", agentConn.Hostname, err.Error())
				errChan <- err
			}
		}(agentConn)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return errors.Errorf("Error creating segment data directories: %s", err.Error())
		}
	}
	return nil
}

func (h *Hub) PrepareInitCluster(ctx context.Context, in *pb.PrepareInitClusterRequest) (*pb.PrepareInitClusterReply, error) {
	gplog.Info("starting PrepareInitCluster()")

	// Read original cluster config file
	oldReader := configutils.NewReader()
	oldReader.OfOldClusterConfig(h.conf.StateDir)

	// Create gpinitsystem_config file for the new cluster from the old config
	gpinitsystemConfig := []string{`ARRAY_NAME="gp_upgrade cluster"`}

	//seg prefix
	mdd := oldReader.GetMasterDataDir()
	segPrefix := path.Base(mdd)
	gplog.Info("Data Dir: %s", mdd)
	gplog.Info("segPrefix: %v", segPrefix)
	segPrefix = segPrefix[:len(segPrefix)-2]
	gpinitsystemConfig = append(gpinitsystemConfig, fmt.Sprintf("SEG_PREFIX=%s_upgrade", segPrefix))

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

	//set segmentDatadirs
	var segmentDatadirs []string
	for _, datadir := range oldReader.GetSegmentDataDirs() {
		dir := fmt.Sprintf("%s_upgrade", path.Dir(datadir))
		segmentDatadirs = append(segmentDatadirs, dir)
	}

	datadirDeclare := fmt.Sprintf("declare -a DATA_DIRECTORY=(%s)", strings.Join(segmentDatadirs, " "))
	gpinitsystemConfig = append(gpinitsystemConfig, datadirDeclare)

	//set master hostname
	hostname, err := os.Hostname()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	gpinitsystemConfig = append(gpinitsystemConfig, fmt.Sprintf("MASTER_HOSTNAME=%s", hostname))

	//set master datadir
	mddBase := path.Dir(oldReader.GetMasterDataDir())
	masterDataDir := fmt.Sprintf("MASTER_DIRECTORY=%s", mddBase)
	gpinitsystemConfig = append(gpinitsystemConfig, masterDataDir)

	//set master port
	oldMasterPort := oldReader.GetPortForSegment(1)
	gplog.Info("oldMasterPort: %v", oldMasterPort)
	masterPort, err := GetNewMasterPort(h.commandExecer, oldMasterPort)
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}
	gpinitsystemConfig = append(gpinitsystemConfig, fmt.Sprintf("MASTER_PORT=%d", masterPort))

	gpinitsystemConfig = append(gpinitsystemConfig, "TRUSTED_SHELL=ssh")
	gpinitsystemConfig = append(gpinitsystemConfig, "CHECK_POINT_SEGMENTS=8")
	gpinitsystemConfig = append(gpinitsystemConfig, "ENCODING=UNICODE")

	gpinitsystemConfigContents := []byte(strings.Join(gpinitsystemConfig, "\n"))
	gpinitsystemConfigFilepath := filepath.Join(h.conf.StateDir,
		"gpinitsystem_config")
	err = ioutil.WriteFile(gpinitsystemConfigFilepath, gpinitsystemConfigContents,
		0644)
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	//write the hostname File
	hostnames, err := oldReader.GetHostnames()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}
	hostnameFilepath := filepath.Join(h.conf.StateDir, "hostfile")
	err = ioutil.WriteFile(hostnameFilepath, []byte(strings.Join(hostnames, "\n")),
		0644)

	// create directories for gpinitsystem
	os.Mkdir(mddBase, 0755)

	err = CreateSegmentDataDirectories(agentConns, segmentDatadirs)
	// Need to created sgement data directories

	// init the new cluster
	cmdStr := fmt.Sprintf("gpinitsystem -a -c %s -h %s", gpinitsystemConfigFilepath,
		hostnameFilepath)
	gpinitsystemCmd := h.commandExecer("bash", "-c", cmdStr)
	_, err = gpinitsystemCmd.Output()
	if err != nil {
		return &pb.PrepareInitClusterReply{}, err
	}

	dbConnector := db.NewDBConn("localhost", int(masterPort), "template1")
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
