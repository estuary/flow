package gazctlcmd

import (
	"bytes"
	"text/template"
)

var journalsEditLongDesc, shardsEditLongDesc string

type editDescription struct {
	Type, HelpCommand, Examples string
}

func init() {
	// Avoid heavy duplication of text between "journals edit" and
	// "shards edit" commands by templating their long descriptions.
	var editTemplate = template.Must(template.New("template").Parse(`Edit and apply {{ .Type }} specifications.

The edit command allows you to directly edit {{ .Type }} specifications matching
the supplied LabelSelector. It will open the editor defined by your GAZ_EDITOR or
EDITOR environment variables or fall back to 'vi'. Editing from Windows is
currently not supported.

Upon exiting the editor, if the file has been changed, it will be validated and
applied. If the file is invalid or fails to apply, the editor is re-opened.
Exiting the editor with no changes or saving an empty file are interpreted as
the user aborting the edit attempt.

Use --selector to supply a LabelSelector which constrains the set of returned
{{ .Type }} specifications. See "{{ .HelpCommand }}" for details and examples.

{{ .Examples }}
`))
	var journalData = editDescription{
		Type:        "journal",
		HelpCommand: "journals list --help",
		Examples: `Edit specifications of journals having an exact name:
>    gazctl journals edit --selector "name in (foo/bar, baz/bing)"

Use an alternative editor
>    GAZ_EDITOR=nano gazctl journals edit --selector "prefix = my/prefix/"`,
	}
	var shardData = editDescription{
		Type:        "shard",
		HelpCommand: "shards list --help",
		Examples: `Edit specifications of shards having an exact ID:
>    gazctl shards edit --selector "id in (foo, bar)"

Use an alternative editor
>    GAZ_EDITOR=nano gazctl shards edit --selector "id = baz"`,
	}

	// Save the template output to package vars.
	var buf = &bytes.Buffer{}
	if err := editTemplate.Execute(buf, journalData); err != nil {
		panic(err)
	}
	journalsEditLongDesc = buf.String() + maxTxnSizeWarning
	buf.Reset()
	if err := editTemplate.Execute(buf, shardData); err != nil {
		panic(err)
	}
	shardsEditLongDesc = buf.String() + maxTxnSizeWarning
}
