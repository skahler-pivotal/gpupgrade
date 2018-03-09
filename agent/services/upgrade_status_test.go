package services_test

import (
	"context"

	"gp_upgrade/agent/services"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CommandListener", func() {
	var (
		testLogFile *gbytes.Buffer
	)

	BeforeEach(func() {
		_, _, testLogFile = testhelper.SetupTestLogger()
	})

	AfterEach(func() {
		//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
		utils.System = utils.InitializeSystemFunctions()
	})

	It("returns the shell command output", func() {
		utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
			return []byte("shell command output"), nil
		}

		listener := services.NewAgentServer()
		resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
		Expect(resp.ProcessList).To(Equal("shell command output"))
		Expect(err).To(BeNil())
	})

	It("returns only err if anything is reported as an error", func() {
		utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
			return []byte("stdout during error"), errors.New("couldn't find bash")
		}

		listener := services.NewAgentServer()
		resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
		Expect(resp).To(BeNil())
		Expect(err.Error()).To(Equal("couldn't find bash"))
		Expect(string(testLogFile.Contents())).To(ContainSubstring("couldn't find bash"))
	})
})
