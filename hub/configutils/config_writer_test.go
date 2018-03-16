package configutils_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gp_upgrade/testutils"

	"gp_upgrade/hub/configutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("configWriter", func() {
	var (
		dir string
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(dir)
	})

	Describe("Load", func() {
		It("initializes a configuration", func() {
			sampleCombinedRows := make([]interface{}, 2)
			sampleCombinedRows[0] = "value1"
			sampleCombinedRows[1] = []byte{35}
			fakeRows := &testutils.FakeRows{
				FakeColumns:      []string{"colnameString", "colnameBytes"},
				NumRows:          1,
				SampleRowStrings: sampleCombinedRows,
			}

			configWriter := configutils.NewWriter(dir, "/tmp/doesnotexist")

			err := configWriter.Load(fakeRows)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(configWriter.TableJSONData)).To(Equal(1))
			Expect(configWriter.TableJSONData[0]["colnameString"]).To(Equal("value1"))
			Expect(configWriter.TableJSONData[0]["colnameBytes"]).To(Equal("#"))
		})

		It("it returns an error if the rows are empty", func() {
			configWriter := configutils.NewWriter(dir, "/tmp/doesnotexist")
			err := configWriter.Load(&sql.Rows{})

			Expect(err).To(HaveOccurred())
		})

		It("returns an error if the given rows do not parse via Columns()", func() {
			sample := make([]interface{}, 1)
			sample[0] = "value1"

			fakeRows := &testutils.FakeRows{
				FakeColumns:      []string{"colname1", "colname2"},
				NumRows:          1,
				SampleRowStrings: sample,
			}
			writer := configutils.NewWriter("", "/tmp/doesnotexist")
			err := writer.Load(fakeRows)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Write", func() {
		const (
			expected_json = `[{"some": "json"}]`
		)

		var (
			json_structure []map[string]interface{}
		)

		BeforeEach(func() {
			err := json.Unmarshal([]byte(expected_json), &json_structure)
			Expect(err).NotTo(HaveOccurred())
		})

		It("writes a configuration when given json", func() {
			writer := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     configutils.NewJSONFormatter(),
				FileWriter:    configutils.NewRealFileWriter(),
				PathToFile:    configutils.GetConfigFilePath(dir),
				BaseDir:       dir,
			}

			err := writer.Write()
			Expect(err).NotTo(HaveOccurred())

			content, err := ioutil.ReadFile(configutils.GetConfigFilePath(dir))
			Expect(err).NotTo(HaveOccurred())
			Expect(string(content)).To(MatchJSON(expected_json))
		})

		It("returns an error when home directory is not writable", func() {
			os.Chmod(dir, 0100)
			writer := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     configutils.NewJSONFormatter(),
				FileWriter:    configutils.NewRealFileWriter(),
				BaseDir:       filepath.Join(dir, ".gp_upgrade"),
			}
			err := writer.Write()

			Expect(err).To(HaveOccurred())
			Expect(string(err.Error())).To(ContainSubstring(fmt.Sprintf("mkdir %v/.gp_upgrade: permission denied", dir)))
		})

		It("returns an error when cluster configutils.go file cannot be opened", func() {
			// pre-create the directory with 0100 perms
			err := os.Chmod(dir, 0100)
			Expect(err).NotTo(HaveOccurred())

			writer := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     configutils.NewJSONFormatter(),
				PathToFile:    configutils.GetConfigFilePath(dir),
				BaseDir:       dir,
			}
			err = writer.Write()

			Expect(err).To(HaveOccurred())
			Expect(string(err.Error())).To(ContainSubstring(fmt.Sprintf("open %v/cluster_config.json: permission denied", dir)))
		})

		It("returns an error when json marshalling fails", func() {
			myMap := make(map[string]interface{})
			myMap["foo"] = make(chan int) // there is no json representation for a channel
			malformed_json_structure := []map[string]interface{}{
				0: myMap,
			}
			writer := configutils.Writer{
				TableJSONData: malformed_json_structure,
				Formatter:     configutils.NewJSONFormatter(),
				BaseDir:       dir,
			}
			err := writer.Write()

			Expect(err).To(HaveOccurred())
		})

		It("returns an error when json pretty print fails", func() {
			writer := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     &testutils.ErrorFormatter{},
			}
			err := writer.Write()

			Expect(err).To(HaveOccurred())
		})

		It("returns an error when file writing fails", func() {
			writer := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     &testutils.NilFormatter{},
				FileWriter:    &testutils.ErrorFileWriterDuringWrite{},
			}
			err := writer.Write()

			Expect(err).To(HaveOccurred())
		})
	})
})
