package configutils

import (
	"os"

	"encoding/json"
	"gp_upgrade/utils"

	"github.com/pkg/errors"
)

type Store interface {
	Load(rows utils.RowsWrapper) error
	Write() error
}

type Writer struct {
	TableJSONData []map[string]interface{}
	Formatter     Formatter
	FileWriter    FileWriter
	PathToFile    string
	BaseDir       string
}

func NewWriter(baseDir, PathToFile string) *Writer {
	return &Writer{
		Formatter:  NewJSONFormatter(),
		FileWriter: NewRealFileWriter(),
		PathToFile: PathToFile,
		BaseDir:    baseDir,
	}
}

func (w *Writer) Load(rows utils.RowsWrapper) error {
	var err error
	w.TableJSONData, err = translateColumnsIntoGenericStructure(rows)
	return err
}

func (w *Writer) Write() error {
	jsonData, err := json.Marshal(w.TableJSONData)
	if err != nil {
		return errors.New(err.Error())
	}

	pretty, err := w.Formatter.Format(jsonData)
	if err != nil {
		return errors.New(err.Error())
	}

	err = os.MkdirAll(w.BaseDir, 0700)
	if err != nil {
		return errors.New(err.Error())
	}

	f, err := os.Create(w.PathToFile)
	if err != nil {
		return errors.New(err.Error())
	}
	defer f.Close()

	err = w.FileWriter.Write(f, pretty)
	if err != nil {
		return errors.New(err.Error())
	}

	return nil
}

func translateColumnsIntoGenericStructure(rows utils.RowsWrapper) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.New(err.Error())
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	return tableData, nil
}
