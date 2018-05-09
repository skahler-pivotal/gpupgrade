package commanders

// This global var UpgradeVersion should have a value set at build time.
// see Makefile for -ldflags "-X etc"
var UpgradeVersion = ""

type VersionCommand struct{}

const DefaultUpgradeVersion = "gpupgrade unknown version"

func VersionString() string {
	if UpgradeVersion == "" {
		return DefaultUpgradeVersion
	}
	return "gpupgrade version " + UpgradeVersion
}
