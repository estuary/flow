ROOT = $(shell pwd)

update-secret:
	sops --decrypt config.sops.yaml | \
		gcloud secrets versions add \
			auth_config_yaml \
			--project estuary-control \
			--data-file=-

deploy:
	gcloud run deploy auth \
		--project estuary-control \
		--source .
