package utils

import (
	"encoding/json"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

const (
	SOURCE_CONFIG_FILENAME = "source_cluster_config.json"
	TARGET_CONFIG_FILENAME = "target_cluster_config.json"
)

type Cluster struct {
	*cluster.Cluster
	BinDir     string
	ConfigPath string
}

/*
 * We need to use an intermediary struct for reading and writing fields not
 * present in cluster.Cluster
 */
type ClusterConfig struct {
	SegConfigs []cluster.SegConfig
	BinDir     string
}

func (c *Cluster) Load() error {
	contents, err := System.ReadFile(c.ConfigPath)
	if err != nil {
		return err
	}
	clusterConfig := &ClusterConfig{}
	err = json.Unmarshal([]byte(contents), clusterConfig)
	if err != nil {
		return err
	}
	c.Cluster = cluster.NewCluster(clusterConfig.SegConfigs)
	c.BinDir = clusterConfig.BinDir
	return nil
}

func (c *Cluster) Commit() error {
	segConfigs := make([]cluster.SegConfig, 0)
	clusterConfig := &ClusterConfig{BinDir: c.BinDir}

	for _, contentID := range c.Cluster.ContentIDs {
		segConfigs = append(segConfigs, c.Segments[contentID])
	}

	clusterConfig.SegConfigs = segConfigs

	return WriteJSONFile(c.ConfigPath, clusterConfig)
}

func (c *Cluster) MasterDataDir() string {
	return c.GetDirForContent(-1)
}

func (c *Cluster) MasterHost() string {
	return c.GetHostForContent(-1)
}

func (c *Cluster) MasterPort() int {
	return c.GetPortForContent(-1)
}

func (c *Cluster) GetHostnames() []string {
	hostnameMap := make(map[string]bool, 0)
	for _, seg := range c.Segments {
		hostnameMap[seg.Hostname] = true
	}
	hostnames := make([]string, 0)
	for host := range hostnameMap {
		hostnames = append(hostnames, host)
	}
	return hostnames
}

func (c *Cluster) NewDBConn() *dbconn.DBConn {
	defaultUser := "gpadmin"

	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, err := operating.System.CurrentUser()
		if err != nil {
			gplog.Verbose("Error retrieving current os user, defaulting to %s", defaultUser)
			username = defaultUser
		} else {
			username = currentUser.Username
		}
	}

	return &dbconn.DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   dbconn.GPDBDriver{},
		User:     username,
		DBName:   "postgres",
		Host:     c.MasterHost(),
		Port:     c.MasterPort(),
		Tx:       nil,
		Version:  dbconn.GPDBVersion{},
	}
}
