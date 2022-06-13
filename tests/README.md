# End to end tests

In order to write an end-to-end test, create a new folder with a flow catalog named `flow.yaml`.

You can then add extra files to run checks:

- `logs` file to check if certain logs exist among all the logs of the deployment, This uses `grep` so you can use patterns supported by `grep` here. This file requires you to specify the columns you want to match against as the first line. Only those columns are fetched from the database. This file also requires you to import `materialize-logs.flow.yaml` in your catalog.
- `<table_name>.local.rows` run a diff against content of `<table_name>` in the local postgres instance. This file requires you to specify the columns you want to match against as the first line. Only those columns are fetched from the database.
- `<table_name>.tunnel.rows` run a diff against content of `<table_name>` in the postgres instance that is behind an SSH tunnel. This file requires you to specify the columns you want to match against as the first line. Only those columns are fetched from the database.
- `data-plane.stdout` and `data-plane.stderr` check if certain lines against among the logs of the data plane. This uses `grep` so you can use patterns supported by `grep` here.
- Similarly, there is `activate.stdout`, `activate.stderr`, `build.stdout`, `build.stderr`, `await.stdout` and `await.stderr`.
