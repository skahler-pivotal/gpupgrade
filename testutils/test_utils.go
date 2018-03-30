package testutils

import (
	"fmt"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"net"
)

const (
	SAMPLE_JSON = `[{
    "address": "briarwood",
    "content": 2,
    "datadir": "/Users/pivotal/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2",
    "dbid": 7,
    "hostname": "briarwood",
    "mode": "s",
    "port": 25437,
    "preferred_role": "m",
    "role": "m",
    "status": "u"
  },
  {
    "address": "aspen",
    "content": 1,
    "datadir": "/Users/pivotal/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1",
    "dbid": 6,
    "hostname": "aspen.pivotal",
    "mode": "s",
    "port": 25436,
    "preferred_role": "m",
    "role": "m",
    "status": "u"
  }]`
)

func Check(msg string, e error) {
	if e != nil {
		panic(fmt.Sprintf("%s: %s\n", msg, e.Error()))
	}
}

func WriteSampleConfig(base string) {
	WriteOldConfig(base, SAMPLE_JSON)
}

func WriteOldConfig(base, jsonConfig string) {
	err := os.MkdirAll(base, 0700)
	Check("cannot create old sample dir", err)
	err = ioutil.WriteFile(configutils.GetConfigFilePath(base), []byte(jsonConfig), 0600)
	Check("cannot write old sample configutils", err)
}

func WriteNewConfig(base, jsonConfig string) {
	err := os.MkdirAll(base, 0700)
	Check("cannot create new sample dir", err)
	err = ioutil.WriteFile(configutils.GetNewClusterConfigFilePath(base), []byte(jsonConfig), 0600)
	Check("cannot write new sample configutils", err)
}

func GetUpgradeStatus(hub *services.HubClient, step pb.UpgradeSteps) (pb.StepStatus, error) {
	reply, err := hub.StatusUpgrade(nil, &pb.StatusUpgradeRequest{})
	stepStatuses := reply.GetListOfUpgradeStepStatuses()
	var stepStatusSaved *pb.UpgradeStepStatus
	for _, stepStatus := range stepStatuses {
		if stepStatus.GetStep() == step {
			stepStatusSaved = stepStatus
		}
	}
	return stepStatusSaved.GetStatus(), err
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
	port := l.Addr().(*net.TCPAddr).Port
	return port, nil
}
