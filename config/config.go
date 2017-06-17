package config

import (
	"os"
	"path"
)

//"address": "briarwood",
//"content": 2,
//"dbid": 7,
//"datadir": "/Users/pivotal/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2",
//"hostname": "briarwood",
//"mode": "s",
//"port": 25437,
//"preferred_role": "m",
//"role": "m",
//"san_mounts": null,
//"status": "u"

type Configuration []ConfigRow

type ConfigRow struct {
	Address  string `json:"address"`
	Content  int    `json:"content"`
	DBID     int    `json:"dbid"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}

func GetConfigDir() string {
	return path.Join(os.Getenv("HOME"), ".gp_upgrade")
}

func GetConfigFilePath() string {
	return path.Join(GetConfigDir(), "cluster_config.json")
}
