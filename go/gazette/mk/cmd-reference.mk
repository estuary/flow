
cmd-reference-targets = \
	docs/_static/cmd-gazette-serve.txt \
	docs/_static/cmd-gazette-print-config.txt \
	docs/_static/cmd-gazctl.txt \
	docs/_static/cmd-gazctl-attach-uuids.txt \
	docs/_static/cmd-gazctl-journals-append.txt \
	docs/_static/cmd-gazctl-journals-apply.txt \
	docs/_static/cmd-gazctl-journals-edit.txt \
	docs/_static/cmd-gazctl-journals-fragments.txt \
	docs/_static/cmd-gazctl-journals-list.txt \
	docs/_static/cmd-gazctl-journals-prune.txt \
	docs/_static/cmd-gazctl-journals-read.txt \
	docs/_static/cmd-gazctl-journals-reset-head.txt \
	docs/_static/cmd-gazctl-print-config.txt \
	docs/_static/cmd-gazctl-shards-apply.txt \
	docs/_static/cmd-gazctl-shards-edit.txt \
	docs/_static/cmd-gazctl-shards-list.txt \
	docs/_static/cmd-gazctl-shards-prune.txt

cmd-reference: ${cmd-reference-targets}


docs/_static/cmd-gazette-serve.txt: go-install
	gazette serve --help > $@ || true
docs/_static/cmd-gazette-print-config.txt: go-install
	gazette serve print config --help > $@ || true

docs/_static/cmd-gazctl.txt: go-install
	gazctl --help > $@ || true
docs/_static/cmd-gazctl-attach-uuids.txt: go-install
	gazctl attach-uuids --help > $@ || true
docs/_static/cmd-gazctl-journals-append.txt: go-install
	gazctl journals append --help > $@ || true
docs/_static/cmd-gazctl-journals-apply.txt: go-install
	gazctl journals apply --help > $@ || true
docs/_static/cmd-gazctl-journals-edit.txt: go-install
	gazctl journals edit --help > $@ || true
docs/_static/cmd-gazctl-journals-fragments.txt: go-install
	gazctl journals fragments --help > $@ || true
docs/_static/cmd-gazctl-journals-list.txt: go-install
	gazctl journals list --help > $@ || true
docs/_static/cmd-gazctl-journals-prune.txt: go-install
	gazctl journals prune --help > $@ || true
docs/_static/cmd-gazctl-journals-read.txt: go-install
	gazctl journals read --help > $@ || true
docs/_static/cmd-gazctl-journals-reset-head.txt: go-install
	gazctl journals reset-head --help > $@ || true
docs/_static/cmd-gazctl-print-config.txt: go-install
	gazctl print-config --help > $@ || true
docs/_static/cmd-gazctl-shards-apply.txt: go-install
	gazctl shards apply --help > $@ || true
docs/_static/cmd-gazctl-shards-edit.txt: go-install
	gazctl shards edit --help > $@ || true
docs/_static/cmd-gazctl-shards-list.txt: go-install
	gazctl shards list --help > $@ || true
docs/_static/cmd-gazctl-shards-prune.txt: go-install
	gazctl shards prune --help > $@ || true

.PHONY: cmd-reference
