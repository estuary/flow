
This connector materializes Flow collections into tables in a PostgreSQL database.

[`ghcr.io/estuary/materialize-postgres:dev`](https://ghcr.io/estuary/materialize-postgres:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Postgres database to which to materialize, and user credentials.
  The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.
* At least one Flow collection

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Postgres materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property        | Title    | Description                                     | Type    | Required/Default |
|-----------------|----------|-------------------------------------------------|---------|------------------|
| `/database`     | Database | Name of the logical database to materialize to. | string  |                  |
| **`/address`**  | Address  | Host and port of the database                   | string  | Required         |
| **`/password`** | Password | Password for the specified database user.       | string  | Required         |
| **`/user`**     | User     | Database user to connect as.                    | string  | Required         |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          database: flow
          address: localhost:5432
          password: flow
          user: flow
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## PostgreSQL on managed cloud platforms

In addition to standard PostgreSQL, this connector supports cloud-based PostgreSQL instances.
To connect securely, you must use an SSH tunnel.

Google Cloud Platform, Amazon Web Service, and Microsoft Azure are currently supported.
You may use other cloud platforms, but Estuary doesn't guarantee performance.


### Setup

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on the cloud platform of your choice.

2. Configure your connector as described in the [configuration](#configuration) section above,
with the additional of the `networkTunnel` stanza to enable the SSH tunnel, if using.
See [Connecting to endpoints on secure networks](../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
for additional details and a sample.

:::tip
You can find the values for `forwardHost` and `forwardPort` in the following locations in each platform's console:
* Amazon RDS: `forwardHost` as Endpoint; `forwardPort` as Port.
* Google Cloud SQL: `forwardHost` as Private IP Address; `forwardPort` is always `5432`. You may need to [configure private IP](https://cloud.google.com/sql/docs/postgres/configure-private-ip) on your database.
* Azure Database: `forwardHost` as Server Name; `forwardPort` under Connection Strings (usually `5432`).
:::

## Reserved words

PostgreSQL has a list of reserved words that must be quoted in order to be used as an identifier.
Flow considers all the reserved words that are marked as "reserved" in any of the columns in the official [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-keywords-appendix.html).

These reserve words are listed in the table below. Flow automatically quotes fields that are in this list.

|Reserved words| | | | |
|---|---|---|---|---|
| abs|	current_transform_group_for_type|	indicator|	order|	sqlexception|
| absolute|	current_user|	initial|	out|	sqlstate|
| acos|	cursor|	initially|	outer|	sqlwarning|
|action|	cycle|	inner|	output|	sqrt|
|add|	datalink|	inout|	over|	start|
|all|	date|	input|	overlaps|	static|
|allocate|	day|	insensitive|	overlay|	stddev_pop|
|alter|	deallocate|	insert|	pad|	stddev_samp|
|analyse|	dec|	int|	parameter|	submultiset|
|analyze|	decfloat|	integer|	partial|	subset|
|and|	decimal|	intersect|	partition|	substring|
|any|	declare|	intersection|	pattern|	substring_regex|
|are|	default|	interval|	per|	succeeds|
|array|	deferrable|	into|	percent|	sum|
|array_agg|	deferred|	is|	percentile_cont|	symmetric|
|array_max_cardinality|	define|	isnull|	percentile_disc|	system|
|as|	delete|	isolation|	percent_rank|	system_time|
|asc|	dense_rank|	join|	period|	system_user|
|asensitive|	deref|	json_array|	permute|	table|
|asin|	desc|	json_arrayagg|	placing|	tablesample|
|assertion|	describe|	json_exists|	portion|	tan|
|asymmetric|	descriptor|	json_object|	position|	tanh|
|at|	deterministic|	json_objectagg|	position_regex|	temporary|
|atan|	diagnostics|	json_query|	power|	then|
|atomic|	disconnect|	json_table|	precedes|	time|
|authorization|	distinct|	json_table_primitive|	precision|	timestamp|
|avg|	dlnewcopy|	json_value|	prepare|	timezone_hour|
|begin|	dlpreviouscopy|	key|	preserve|	timezone_minute|
|begin_frame|	dlurlcomplete|	lag|	primary|	to|
|begin_partition|	dlurlcompleteonly|	language|	prior|	trailing|
|between|	dlurlcompletewrite|	large|	privileges|	transaction|
|bigint|	dlurlpath|	last|	procedure|	translate|
|binary|	dlurlpathonly|	last_value|	ptf|	translate_regex|
|bit|	dlurlpathwrite|	lateral|	public|	translation|
|bit_length|	dlurlscheme|	lead|	range|	treat|
|blob|	dlurlserver|	leading|	rank|	trigger|
|boolean|	dlvalue|	left|	read|	trim|
|both|	do|	level|	reads|	trim_array|
|by|	domain|	like|	real|	true|
|call|	double|	like_regex|	recursive|	truncate|
|called|	drop|	limit|	ref|	uescape|
|cardinality|	dynamic|	listagg|	references|	union|
|cascade|	each|	ln|	referencing|	unique|
|cascaded|	element|	local|	regr_avgx|	unknown|
|case|	else|	localtime|	regr_avgy|	unmatched|
|cast|	empty|	localtimestamp|	regr_count|	unnest|
|catalog|	end|	log|	regr_intercept|	update|
|ceil|	end-exec|	log10|	regr_r2|	upper|
|ceiling|	end_frame|	lower|	regr_slope|	usage|
|char|	end_partition|	match|	regr_sxx|	user|
|character|	equals|	matches|	regr_sxy|	using|
|character_length|	escape|	match_number|	regr_syy|	value|
|char_length|	every|	match_recognize|	relative|	values|
|check|	except|	max|	release|	value_of|
|classifier|	exception|	measures|	restrict|	varbinary|
|clob|	exec|	member|	result|	varchar|
|close|	execute|	merge|	return|	variadic|
|coalesce|	exists|	method|	returning|	varying|
|collate|	exp|	min|	returns|	var_pop|
|collation|	external|	minute|	revoke|	var_samp|
|collect|	extract|	mod|	right|	verbose|
|column|	false|	modifies|	rollback|	versioning|
|commit|	fetch|	module|	rollup|	view|
|concurrently|	filter|	month|	row|	when|
|condition|	first|	multiset|	rows|	whenever|
|connect|	first_value|	names|	row_number|	where|
|connection|	float|	national|	running|	width_bucket|
|constraint|	floor|	natural|	savepoint|	window|
|constraints|	for|	nchar|	schema|	with|
|contains|	foreign|	nclob|	scope|	within|
|continue|	found|	new|	scroll|	without|
|convert|	frame_row|	next|	search|	work|
|copy|	free|	no|	second|	write|
|corr|	freeze|	none|	section|	xml|
|corresponding|	from|	normalize|	seek|	xmlagg|
|cos|	full|	not|	select|	xmlattributes|
|cosh|	function|	notnull|	sensitive|	xmlbinary|
|count|	fusion|	nth_value|	session|	xmlcast|
|covar_pop|	get|	ntile|	session_user|	xmlcomment|
|covar_samp|	global|	null|	set|	xmlconcat|
|create|	go|	nullif|	show|	xmldocument|
|cross|	goto|	numeric|	similar|	xmlelement|
|cube|	grant|	occurrences_regex|	sin|	xmlexists|
|cume_dist|	group|	octet_length|	sinh|	xmlforest|
|current|	grouping|	of|	size|	xmliterate|
|current_catalog|	groups|	offset|	skip|	xmlnamespaces|
|current_date|	having|	old|	smallint|	xmlparse|
|current_default_transform_group|	hold|	omit|	some|	xmlpi|
|current_path|	hour|	on|	space|	xmlquery|
|current_role|	identity|	one|	specific|	xmlserialize|
|current_row|	ilike|	only|	specifictype|	xmltable|
|current_schema|	immediate|	open|	sql|	xmltext|
|current_time|	import|	option|	sqlcode|	xmlvalidate|
|current_timestamp|	in|	or|	sqlerror|	year|
