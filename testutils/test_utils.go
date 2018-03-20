package testutils

import (
	"fmt"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"

	"io/ioutil"
	"os"
)

const (
	TempHomeDir = "/tmp/gp_upgrade_test_temp_home_dir"

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
	WriteProvidedConfig(base, SAMPLE_JSON)
}

func WriteProvidedConfig(base, jsonConfig string) {
	err := os.MkdirAll(base, 0700)
	Check("cannot create sample dir", err)
	err = ioutil.WriteFile(configutils.GetConfigFilePath(base), []byte(jsonConfig), 0600)
	Check("cannot write sample configutils", err)
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
