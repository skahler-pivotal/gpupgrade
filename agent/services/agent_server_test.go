package services

import (
	"gp_upgrade/utils"

	. "github.com/onsi/ginkgo"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
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

})
