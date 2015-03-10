package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/api-server/logging"
)

type Service struct {
	Journals []*Journal // Sorted on Journal.Name.

	GCSContext *logging.GCSContext
}

func (s *Service) OnEtcdResponse(response *etcd.Response, tree *etcd.Node) {
	journalModels := extractJournalNodes(tree)

	for i, model := range journalModels {
		// Add and drop journals as needed, until |s.Journals| matches
		// the form of |journalModels|.
		for i == len(s.Journals) || s.Journals[i].Name != model.Name {
			if i == len(s.Journals) {
				// Insert a new journal at the tail.
				s.Journals = append(s.Journals, &Journal{Name: model.Name})
				log.WithField("name", model.Name).Info("tracking journal")
			} else if s.Journals[i].Name < model.Name {
				log.WithField("name", s.Journals[i].Name).Info("dropping journal")

				copy(s.Journals[i:], s.Journals[i+1:])
				s.Journals = s.Journals[:len(s.Journals)-1]
			} else if s.Journals[i].Name > model.Name {
				// Splice in a new journal.
				s.Journals = append(s.Journals, nil)
				copy(s.Journals[i+1:], s.Journals[i:])

				s.Journals[i] = &Journal{Name: model.Name}
				log.WithField("name", model.Name).Info("tracking journal")
			}
		}
		//s.updateJournal(s.Journals[i], model.Node)
	}
	for len(s.Journals) > len(journalModels) {
		i := len(s.Journals) - 1
		log.WithField("name", s.Journals[i].Name).Info("dropping journal")
		s.Journals = s.Journals[:i]
	}
}

type journalNode struct {
	Name string
	Node *etcd.Node
}

// Identifies and returns (in sorted order) etcd Nodes which represent journals.
func extractJournalNodes(tree *etcd.Node) []journalNode {
	var result []journalNode

	// Walk the tree. Journals are represented in etcd as directory
	// nodes with no sub-directory children.
	stack := []etcd.Nodes{tree.Nodes}
	for i := len(stack); i != 0; i = len(stack) {
		if j := len(stack[i-1]); j == 0 {
			stack = stack[:i-1]
			continue
		}
		node := stack[i-1][0]
		stack[i-1] = stack[i-1][1:] // Pop.

		if len(node.Nodes) != 0 {
			stack = append(stack, node.Nodes)
		}
		var hasSubDir bool
		for _, sub := range node.Nodes {
			if sub.Dir {
				hasSubDir = true
				break
			}
		}
		if !node.Dir || hasSubDir {
			continue
		}
		result = append(result, journalNode{node.Key[len(tree.Key):], node})
	}
	return result
}
