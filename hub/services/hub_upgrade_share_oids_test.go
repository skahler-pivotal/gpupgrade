package services_test

import (
	"errors"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	Describe("UpgradeShareOids", func() {
		var (
			reader        *spyReader
			hub           *services.HubClient
			dir           string
			commandExecer *testutils.FakeCommandExecer
		)

		BeforeEach(func() {
			reader = &spyReader{
				hostnames: []string{"hostone", "hosttwo"},
				segmentConfiguration: configutils.SegmentConfiguration{
					{
						Content:  0,
						DBID:     2,
						Hostname: "hostone",
					}, {
						Content:  1,
						DBID:     3,
						Hostname: "hosttwo",
					},
				},
			}
			var err error
			dir, err = ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			commandExecer = &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{})
			hub = services.NewHub(nil, reader, grpc.DialContext, commandExecer.Exec, &services.HubConfig{
				StateDir: dir,
			})
			testhelper.SetupTestLogger()
		})

		AfterEach(func() {
			os.RemoveAll(dir)
			utils.InitializeSystemFunctions()
		})

		It("Reports status PENDING when no share-oids request has been made", func() {
			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_PENDING))
		})

		It("marks step as FAILED if rsync fails for all hosts", func() {
			numHosts := len(reader.hostnames)

			//numInvocations := 0
			//utils.System.ExecCmdOutput = func(string, ...string) ([]byte, error) {
			//	numInvocations++
			//	if numInvocations == 1 {
			//		return nil, errors.New("failed")
			//	} else {
			//		return []byte("soemthing"), nil
			//	}
			//}
			fakeCommand := &testutils.FakeCommand{
				Out: nil,
				Err: errors.New("failed"),
			}
			commandExecer.SetOutput(fakeCommand)

			hub.ShareOidFilesStub(dir)
			Eventually(fakeCommand.GetNumInvocations()).Should(Equal(numHosts))

			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_FAILED))

		})

		It("marks step as COMPLETE if rsync succeeds for all hosts", func() {
			numHosts := len(reader.hostnames)

			//var numInvocations int
			//utils.System.ExecCmdOutput = func(string, ...string) ([]byte, error) {
			//	numInvocations++
			//	return []byte("success"), nil
			//}
			fakeCommand := &testutils.FakeCommand{
				Out: []byte("success"),
				Err: nil,
			}
			commandExecer.SetOutput(fakeCommand)

			hub.ShareOidFilesStub(dir)
			Eventually(fakeCommand.GetNumInvocations()).Should(Equal(numHosts))

			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_COMPLETE))
		})
	})
})
