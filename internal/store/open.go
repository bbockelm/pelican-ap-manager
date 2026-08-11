package store

import (
	"fmt"

	"github.com/bbockelm/golang-htcondor/config"
)

// Options selects and configures a rule store backend.
type Options struct {
	// DBAddress, when non-empty, selects the htcondordb backend and names the
	// daemon to reach.
	DBAddress string
	// DBTable names the rule table in htcondordb. Defaults to DefaultRuleTable.
	DBTable string
	// FilePath is the JSON document used when DBAddress is empty.
	FilePath string
	// Config supplies the client security policy for the htcondordb backend.
	Config *config.Config
}

// Open returns the configured rule store, along with a description of the
// backend suitable for a startup log line.
func Open(opts Options) (RuleStore, string, error) {
	if opts.DBAddress != "" {
		s, err := OpenDBStore(DBConfig{Address: opts.DBAddress, Table: opts.DBTable, Config: opts.Config})
		if err != nil {
			return nil, "", err
		}
		table := opts.DBTable
		if table == "" {
			table = DefaultRuleTable
		}
		return s, fmt.Sprintf("htcondordb %s table %s", opts.DBAddress, table), nil
	}

	if opts.FilePath == "" {
		return nil, "", fmt.Errorf("store: neither an htcondordb address nor a rule file path is configured")
	}
	s, err := OpenFileStore(opts.FilePath)
	if err != nil {
		return nil, "", err
	}
	return s, "file " + opts.FilePath, nil
}
