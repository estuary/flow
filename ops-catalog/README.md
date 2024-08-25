
# Data-plane names:

Data-planes are placed under ops/public/ or ops/private/

* Public data-planes are placed under `ops/dp/public/$data-plane-name`
* Private data-planes are placed under `ops/dp/private/$tenant/$data-plane-name`.

All tenants in the system are able to access ops/public/.
Designated tenants will be assigned role grants for access to `ops/dp/private/$tenant/`

By convention only, data-plane names are built as `$provider-$region-c$N`,
where `$provider` is `gcp`, `aws`, etc; `$region` is the provider-defined region name,
and `$N` is a cluster number within the region (to allow for multiple data-planes).
Typically `$N` is always 1.

### Examples:

```
ops/dp/public/gcp-us-central1-c1
ops/dp/public/aws-eu-west-1-c2
ops/dp/private/acmeCo/aws-eu-west-2-c1
```

### Discussion:

We can enable RBAC by giving every tenant the following new read grants during onboarding:
* `ops/dp/public/` for access to any public data-planes.
* `ops/dp/private/$tenant/` for access to tenant-specific data-planes that may exist (usually they won't).


# Task logs and stats:

Every data-plane has its own collections for logs and stats, which are placed within the data-plane.

The naming convention matches data-plane naming, except that `dp` => `tasks`.
For example, data-plane `ops/dp/private/acmeCo/aws-eu-west-2-c1` implies
a task logs collection of `ops/tasks/private/acmeCo/aws-eu-west-2-c1/logs`.

### Examples:

```
ops/tasks/public/gcp-us-central1-c1/logs
ops/tasks/public/gcp-us-central1-c1/stats

ops/tasks/public/aws-eu-west-1-c2/logs
ops/tasks/public/aws-eu-west-1-c2/stats

ops/tasks/private/acmeCo/aws-eu-west-2-c1/logs
ops/tasks/private/acmeCo/aws-eu-west-2-c1/stats
```

### Discussion:

We can enable RBAC to logs and stats of private data-planes with a grant of `ops/tasks/private/$tenant/`,
to allow for direct logs and stats access of a tenants workloads within the data-plane.

In the future, we want to allow tenants to access their authorized raw logs and stats.
This requires a partition-based authorization mechanism which isn't supported today.
However, if and when that's available, we could then allow a blanket
grant of `ops/tasks/public/` to every tenant.

This naming convention also lets us assume that anything under `ops/tasks/`
is a logs or stats collection.

# L1 Rollups

Our reporting is built around a two-stage hierarchy of roll-ups.

Today, we have the following types of roll-ups of raw logs and stats:
* inferred-schemas examines raw logs for updates to inferred schemas
* failure-notifications examines raw logs for failure notifications
* catalog-stats examines raw stats _and_ logs for billable reporting metrics

The first level (L1) of each roll-up happens **inside** the data-plane that homes the raw logs & stats.
This is desired to avoid data-movement and reduce the sheer number of journal partitions each rollup task must read.

These rollups are named after the data-plane name under an `ops/rollups/L1/` prefix.

### Examples:

```
ops/rollups/L1/public/gcp-us-central1-c1/catalog-stats
ops/rollups/L1/public/gcp-us-central1-c1/inferred-schemas
ops/rollups/L1/private/acmeCo/aws-eu-west-2-c1/failure-notifications
```

### Discussion:

Generally these wouldn't need to be shared, but could be for private data-plane tenants.
L1 rollups are not partitioned, so cannot be shared or authorized in the general public case.

# L2 Rollups

Second-level (L2) roll-ups merge the contributions of data-plane-specific
L1 roll-ups into a comprehensive, global data product,
which is used for reporting stats materializations.

These already exist and -- to avoid unneccessary migration right now -- we'll leave them in place.
New L1 rollups will be added as transforms of these existing derivations:

* `ops.us-central1.v1/catalog-stats-L2`
* `ops.us-central1.v1/inferred-schemas/L2`
