# TimescaleDB

This connector materializes Flow collections into tables in a TimescaleDB database.
Timescale DB provides managed PostgreSQL instances for real-time data.
The connector is derived from the main [PostgreSQL](./PostgreSQL.md) materialization connector
and has the same configuration.

The connector is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-timescaledb:dev`](https://ghcr.io/estuary/materialize-timescaledb:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A TimescaleDB database to which to materialize. Know your user credentials, and the host and port.
  If using Timescale Cloud, this information is available on your console, on the **Connection info** pane.

* At least one Flow collection.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a TimescaleDB materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.

### Properties

#### Endpoint

| Property        | Title    | Description                                     | Type    | Required/Default |
|-----------------|----------|-------------------------------------------------|---------|------------------|
| `/database`     | Database | Name of the logical database to materialize to. | string  |                  |
| **`/address`**  | Address  | Host and port of the database                   | string  | Required         |
| **`/password`** | Password | Password for the specified database user.       | string  | Required         |
| `/schema` | Database Schema | Database [schema](https://docs.timescale.com/timescaledb/latest/how-to-guides/schema-management/) to use for materialized tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | `"public"` |
| **`/user`**     | User     | Database user to connect as.                    | string  | Required         |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |
| `/schema` | Alternative Schema | Alternative schema for this table (optional). Overrides schema set in endpoint configuration. | string |  |
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-timescaledb:dev
        config:
          database: flow
          address: xxxxxxxxxx.xxxxxxxxxx.tsdb.cloud.timescale.com:01234
          password: flow
          user: flow
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector supports both standard (merge) and [delta updates](../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

## Reserved words

PostgreSQL (and thus TimescaleDB) has a list of reserved words that must be quoted in order to be used as an identifier.
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

## Changelog

The changelog includes a list of breaking changes made to this connector. Backwards-compatible changes are not listed.

**Proceed with caution when editing materializations created with previous versions of this connector;
editing always upgrades your materialization to the latest connector version.**

#### V4: 2022-11-30

This version includes breaking changes to materialized table columns.
These  provide more consistent column names and types, but tables created from previous versions of the connector may
not be compatible with this version.

* Capitalization is now preserved when fields in Flow are converted to Postgres (TimescaleDB) column names.
  Previously, fields containing uppercase letters were converted to lowercase.

* Field names and values of types `date`, `duration`, `ipv4`, `ipv6`, `macaddr`, `macaddr8`, and `time` are now converted into
  their corresponding Postgres (TimescaleDB) types.
  Previously, only `date-time` was converted, and all others were materialized as strings.
