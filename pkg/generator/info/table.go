package info

type Table struct {
	Name    string `field:"table_name"`
	Columns map[string]Column
	Parents []*Table
}
