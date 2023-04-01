
## On master branch

- `tilt up`
- Install connectors

```
./scripts/install-connector-local.sh ghcr.io/estuary/source-http-ingest v1
./scripts/install-connector-local.sh ghcr.io/estuary/materialize-sqlite v1
```

- Publish ops catalog
- Grant alice access to ops catalog:
  ```
  insert into role_grants (subject_role, object_role, capability) values ('aliceCo/', 'ops.us-central1.v1/', 'admin');
  ```
- publish a catalog with a basic capture, derivation, and materialization:
  ```
  flowctl --profile local catalog publish --source-dir version-test
  ```
- Ingested some docs, verified everything seems working, including:
  - stats in UI: ~5 docs ~3KiB
  - connector networking working for both capture and materialization
  - note that derivation stats are doubled in the UI, and also counting ack documents in the runtime
    - ack docs are not counted on the `johnny/more-sql` branch, so we'll expect these stats to be a little different
- Prepare to transition to the new version by pulling the ops catalog specs into `new-version-test` so I can update them:
  ```
  cd ../new-version-test
  flowctl --profile local catalog pull-specs --prefix ops
  ```
- Leave things running


## Switch to new flow version

- `git co johnny/more-sql`
- disable reactor and agent in tilt (leave broker running so data isn't lost)
- `make` to rebuild binaries
- update connector tags
  ```
  ./scripts/install-connector-local.sh ghcr.io/estuary/source-http-ingest d469aa2
  ./scripts/install-connector-local.sh ghcr.io/estuary/materialize-sqlite d469aa2
  ./scripts/install-connector-local.sh ghcr.io/estuary/materialize-postgres d469aa2
  ```
- re-enable reactor and agent in tilt
  - Reactor needs to go _before_ agent because agent will fail if it can't query the builds root from the reactor
- Update new-version-test/aliceCo/* to use new iamges and new derivation spec structure
- Change local ops catalog template to use new image (derivation specs are already updated) (`git stash apply`)
- Re-publish ops catalog
- Re-publish new-version-test/aliceCo/*


