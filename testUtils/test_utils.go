package testUtils

import (
	"fmt"
	"gp_upgrade/hub/configutils"
	"io/ioutil"
	"os"

	"path"
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

func ResetTempHomeDir() string {
	configDir := path.Join(TempHomeDir, ".gp_upgrade")
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		err = os.Chmod(configDir, 0700)
		Check("cannot change mod", err)
	}
	err := os.RemoveAll(TempHomeDir)
	Check("cannot remove temp home", err)
	save := os.Getenv("HOME")
	err = os.MkdirAll(TempHomeDir, 0700)
	Check("cannot create home temp dir", err)
	err = os.Setenv("HOME", TempHomeDir)
	Check("cannot set home dir", err)
	return save
}

func WriteSampleConfig() {
	err := os.MkdirAll(configutils.GetConfigDir(), 0700)
	Check("cannot create sample dir", err)
	err = ioutil.WriteFile(configutils.GetConfigFilePath(), []byte(SAMPLE_JSON), 0600)
	Check("cannot write sample configutils", err)
}
