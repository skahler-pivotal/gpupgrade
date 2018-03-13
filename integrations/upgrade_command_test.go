package integrations_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("upgrade", func() {
	Describe("share-oids", func() {
		It("updates status PENDING to RUNNING then to COMPLETE if successful", func() {
			runCommand("check", "config")
			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Copy OID files from master to segments"))

			//expectationsDuringCommandInFlight := make(chan bool)

			//go func() {
			//	defer GinkgoRecover()
			//	// TODO: Can this flake? if the in-progress window is shorter than the frequency of Eventually(), then yea
			//	Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Copy OID files from master to segments"))
			//	//close channel here
			//	expectationsDuringCommandInFlight <- true
			//}()

			session := runCommand("upgrade", "share-oids")
			Eventually(session).Should(Exit(0))
			//<-expectationsDuringCommandInFlight

			Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Copy OID files from master to segments"))
		})
	})
})
