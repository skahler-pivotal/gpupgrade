package utils_test

import (
	"io/ioutil"
	"os"
	"os/user"
	"path"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/pkg/errors"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
	var (
		expectedCluster *utils.Cluster
		testStateDir    string
		err             error
	)

	BeforeEach(func() {
		testStateDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		testhelper.SetupTestLogger()
		expectedCluster = &utils.Cluster{
			Cluster:    testutils.CreateMultinodeSampleCluster("/tmp"),
			BinDir:     "/fake/path",
			ConfigPath: path.Join(testStateDir, "cluster_config.json"),
		}
	})

	AfterEach(func() {
		os.RemoveAll(testStateDir)
	})

	Describe("Commit and Load", func() {
		It("can save a config and successfully load it back in", func() {
			err := expectedCluster.Commit()
			Expect(err).ToNot(HaveOccurred())
			givenCluster := &utils.Cluster{
				ConfigPath: path.Join(testStateDir, "cluster_config.json"),
			}
			err = givenCluster.Load()
			Expect(err).ToNot(HaveOccurred())

			// We don't serialize the Executor
			givenCluster.Executor = expectedCluster.Executor

			Expect(expectedCluster).To(Equal(givenCluster))
		})
	})

	Describe("NewDBConn", func() {
		var (
			originalEnv string
			c           *utils.Cluster
		)

		BeforeEach(func() {
			master := cluster.SegConfig{
				DbID:      1,
				ContentID: -1,
				Port:      5432,
				Hostname:  "mdw",
			}

			originalEnv = os.Getenv("PGUSER")

			cc := cluster.Cluster{Segments: map[int]cluster.SegConfig{-1: master}}
			c = &utils.Cluster{Cluster: &cc}

		})

		AfterEach(func() {
			os.Setenv("PGUSER", originalEnv)
			operating.System = operating.InitializeSystemFunctions()
		})

		It("can construct a dbconn from a cluster", func() {
			expectedUser := "brother_maynard"
			os.Setenv("PGUSER", expectedUser)

			dbConnector := c.NewDBConn()

			Expect(dbConnector.DBName).To(Equal("postgres"))
			Expect(dbConnector.Host).To(Equal("mdw"))
			Expect(dbConnector.Port).To(Equal(5432))
			Expect(dbConnector.User).To(Equal(expectedUser))
		})

		It("sets the default user if neither PGUSER or current user are available", func() {
			os.Unsetenv("PGUSER")

			operating.System.CurrentUser = func() (*user.User, error) {
				return nil, errors.New("Your systems is seriously borked if this fails")
			}

			dbConnector := c.NewDBConn()

			Expect(dbConnector.User).To(Equal("gpadmin"))
		})
		// FIXME: protect against badly initialized clusters
	})

})
