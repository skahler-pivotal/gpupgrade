package commanders_test

import (
	"github.com/greenplum-db/gpupgrade/cli/commanders"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Version", func() {
	Describe("VersionString", func() {
		Context("when global var UpgradeVersion is the empty string", func() {
			It("returns the default version", func() {
				commanders.UpgradeVersion = ""
				Expect(commanders.VersionString()).To(Equal("gpupgrade unknown version"))
			})
		})

		Context("when global var UpgradeVersion is set to something", func() {
			It("returns what it's set to", func() {
				commanders.UpgradeVersion = "Something"
				Expect(commanders.VersionString()).To(Equal("gpupgrade version Something"))
			})
		})
	})
})
