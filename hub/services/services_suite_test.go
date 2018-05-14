package services_test

import (
	"testing"

	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var T *testing.T

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	T = t
	RunSpecs(t, "Hub Services Suite")
}

var _ = BeforeSuite(func() {
	testhelper.SetupTestLogger()
})

var _ = AfterEach(func() {
	utils.System = utils.InitializeSystemFunctions()
})
