package services_test

import (
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"errors"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("object count tests", func() {
	var (
		listener                    pb.CliToHubServer
		fakeShutdownClustersRequest *pb.PrepareShutdownClustersRequest
		stdout                      *gbytes.Buffer
	)

	BeforeEach(func() {
		stdout, _, _ = testhelper.SetupTestLogger()
	})
	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("PrepareShutdownClusters", func() {
		Describe("ignoring the go routine", func() {
			initialSetup := func() (pb.CliToHubServer, *pb.PrepareShutdownClustersRequest) {
				listener := services.NewCliToHubListener(&fakeStubClusterPair{})

				fakeShutdownClustersRequest := &pb.PrepareShutdownClustersRequest{OldBinDir: "/old/path/bin",
					NewBinDir: "/new/path/bin"}

				return listener, fakeShutdownClustersRequest
			}

			BeforeEach(func() {
				listener, fakeShutdownClustersRequest = initialSetup()
			})

			It("returns successfully", func() {
				utils.System.Getenv = func(s string) string { return "foo" }
				utils.System.RemoveAll = func(s string) error { return nil }
				utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

				_, err := listener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).To(BeNil())
				Eventually(stdout.Contents()).Should(ContainSubstring("starting PrepareShutdownClusters()"))
			})

			It("fails if home directory not available in environment", func() {
				utils.System.Getenv = func(s string) string { return "" }

				_, err := listener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("home directory environment variable"))
				Eventually(stdout.Contents()).Should(ContainSubstring("starting PrepareShutdownClusters()"))
			})

			It("fails if the cluster configuration setup can't be loaded", func() {
				utils.System.Getenv = func(s string) string { return "foo" }
				utils.System.RemoveAll = func(s string) error { return nil }
				utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

				failingListener := services.NewCliToHubListener(&fakeFailingClusterPair{})

				_, err := failingListener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("boom"))
				Eventually(stdout.Contents()).Should(ContainSubstring("starting PrepareShutdownClusters()"))
			})
		})
	})
})

type fakeStubClusterPair struct{}

func (c *fakeStubClusterPair) StopEverything(str string)                 {}
func (c *fakeStubClusterPair) Init(oldPath string, newPath string) error { return nil }

type fakeFailingClusterPair struct{}

func (c *fakeFailingClusterPair) StopEverything(str string) {}
func (c *fakeFailingClusterPair) Init(oldPath string, newPath string) error {
	return errors.New("boom")
}
