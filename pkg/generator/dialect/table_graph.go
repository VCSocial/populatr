package dialect

import (
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
)

type tableNode struct {
	id      string
	columns map[string]info.ColumnMetadata
	edges   map[string]bool
}

func (tn *tableNode) toTableMetadata() info.TableMetadata {
	return info.TableMetadata{
		Name:    tn.id,
		Columns: tn.columns,
	}
}

type tableGraph struct {
	nodes map[string]*tableNode
}

func newTableGraph() *tableGraph {
	return &tableGraph{nodes: make(map[string]*tableNode)}
}

func (g *tableGraph) Exists(id string) bool {
	_, ok := g.nodes[id]
	return ok
}

func (g *tableGraph) AddNode(id string, columns map[string]info.ColumnMetadata) {
	if g.Exists(id) {
		return
	}

	g.nodes[id] = &tableNode{
		id:      id,
		columns: columns,
		edges:   make(map[string]bool),
	}
}

func (g *tableGraph) AddEdge(orgnId string, destId string) {
	g.nodes[orgnId].edges[destId] = true
}

func (g *tableGraph) AddRef(id string, colName string, ref info.Reference) {
	if c, ok := g.nodes[id].columns[colName]; ok {
		c.Reference = ref
		g.nodes[id].columns[colName] = c
	}
}

func (g *tableGraph) topologicalSortInternal(id string, visited map[string]bool,
	sorted *[]*tableNode) {
	visited[id] = true

	node, ok := g.nodes[id]
	if !ok {
		logging.Global.Error().
			Str("node_id", id).
			Msg("could not find node")
		return
	}

	for edgeId := range node.edges {
		if _, ok := visited[edgeId]; !ok {
			g.topologicalSortInternal(edgeId, visited, sorted)
		}
	}
	*sorted = append(*sorted, node)
}

func (g *tableGraph) topologicalSort() []info.TableMetadata {
	visited := make(map[string]bool)
	sorted := []*tableNode{}
	for id := range g.nodes {
		if _, ok := visited[id]; !ok {
			g.topologicalSortInternal(id, visited, &sorted)
		}
	}

	tables := []info.TableMetadata{}
	for i := len(sorted) - 1; i >= 0; i-- {
		tables = append(tables, sorted[i].toTableMetadata())
		logging.Global.Debug().
			Int("position", i).
			Str("table_name", sorted[i].id)
	}
	return tables
}
