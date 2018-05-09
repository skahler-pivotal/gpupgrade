# gpupgrade

## Developer Workflow

### Prerequisites

- Golang. We currently develop against latest stable Golang, which was v1.10 as of May 2018.
- protoc. This is the compiler for the [gRPC protobuffer](https://grpc.io/) system. 
On macos, one way to install this is via `brew install protobuf`   

### Build and test the upgrade tool

```
make
```
This command will install dependencies, build gpupgrade, and run tests.

### Dependency vendoring

gpupgrade uses go dep to vendor its dependencies.

To view the state of dependencies in the project, use the command
```
dep status
```

To add add new dependencies, use the command
```
dep ensure -add [package import string]
```

To update existing dependencies, use the command
```
dep ensure -update
```

For additional information please refer to the [dep documentation](https://golang.github.io/dep/docs/daily-dep.html)

### Build details

```
make build
```
builds the code without running the tests.

We build with a ldflag to set the version of the code based on the current
git SHA and latest tag at build time.

We build into $GOPATH/bin/gpupgrade and expect that, after a build,
`which gpupgrade` points to the binary that you just built, assuming
your PATH is configured correctly.

We build as part of our (integration tests)[#integration-testing]; see more
information there.

We support cross-compilation into Linux or Darwin, as the GPDB servers that
this tool upgrades run Linux but many dev workstations run MacOS

For a target-specific build, run
```
make build_linux
```
or
```
make build_mac
```
as appropriate for the target platform.

### Run the tests

We use [ginkgo](https://github.com/onsi/ginkgo) and [gomega](https://github.com/onsi/gomega) to run our tests. We have `unit` and `integration` targets predefined.

***Note:*** In order to run integration tests you need to have GPDB installed and running. Instructions to set up a GPDB cluster can be found in [the main GPDB repo](https://github.com/greenplum-db/gpdb).

#### Unit tests
```
# To run all the unit tests
make unit
```
#### Integration tests
```
# To run all the integration tests
make integration
```
#### All tests
```
# To run all the tests
make test
```

### Generate mocked gRPC client/server code
```
# To generate mocked files
make protobuf
```

## Command line parsing

We are using [the cobra library](https://github.com/spf13/cobra) for
parsing our commands and flags.

To implement a new command, you will need to implement a commander (see the cobra documentation and files in the `cli/commanders` directory for examples) and add it to the tree of commands in `cli/gpupgrade_main.go` to tell the parser about your new command.

## Testing

### Unit testing overall

```
make unit
```
should only run the unit tests

We use Ginkgo and Gomega because they provide BDD-style syntax while still
running `go test` under the hood. Core team members strive to TDD the code as
much as possible so that the unit test coverage is driven out alongside the code

We use dependency injection wherever possible to enable isolated unit tests
and drive towards clear interfaces across packages

We keep our `_test.go` files in the same package as the implementations they test because
occasionally, we find it easiest and most valuable to either:

- unit test private methods
- make assertions about the internal state of the struct

We selected this unit testing approach rather than these alternatives:

- putting unit tests in a different package and then needing to make many more functions
  and struct attributes public than necessary
- defining dependencies that aren't under test as `var`s and then redefining
  them at test-time

### Integration testing

```
make integration
```
should only run the integration tests

In order to run the integration tests, a Greenplum Database cluster must be up and
running.
We typically integration test the "happy path" (expected behavior) of the code
when writing new features and allow the unit tests to cover error messaging
and other edge cases. We are not strict about outside-in (integration-first)
or inside-out (unit-first) TDD.

Integration tests here signify end-to-end testing from the outside, starting
with a call to an actual gpupgrade binary. Therefore, the integration tests
do their own `Build()` of the code, using the gomega `gexec` library.

The default integration tests do not build with the special build flags that
the Makefile uses because the capability of the code to react to those build
flags is specifically tested where needed, for example in
[version_integration_test.go](integrations/version_integration_test.go)

The integration tests may require other binaries to be built. We aim to have
any such requirements automated.

### Directly using pg_upgrade

Under the covers, gpupgrade is calling pg_upgrade, first on the master, and
then on the segments. If needed, you can call pg_upgrade directly. There is
make target that runs a test, upgrading from version x to x. To do this, two
clusters are setup on the local machine using demo_cluster.sh. In the root
directory for the gpdb repo, run is `make -C contrib/pg_upgrade check`. This
uses test_gpdb.sh to do the heavy lifting, and that can be customized to fit
your setup. In particular, four env vars are usefor the cluster mapping:
NEWBINDIR, OLDBINDIR, NEWDATADIR and OLDDATADIR.
