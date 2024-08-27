
# AlloyDB

This connector materializes Flow collections into tables in an AlloyDB database.

AlloyDB is a fully managed, PostgreSQL-compatible database available in the Google Cloud platform.
This connector is derived from the [PostgreSQL materialization connector](/reference/Connectors/materialization-connectors/PostgreSQL/),
so the same configuration applies, but the setup steps look somewhat different.

It's available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-alloydb:dev`](https://ghcr.io/estuary/materialize-alloydb:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* An AlloyDB database to which to materialize, and user credentials.
  The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.
* A virtual machine to connect securely to the instance via SSH tunneling. (AlloyDB doesn't support IP allowlisting.)
Follow the instructions to create a [virtual machine for SSH tunneling](../../../guides/connect-network.md#setup-for-google-cloud)
in the same Google Cloud project as your instance.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a AlloyDB materialization, which will direct one or more of your Flow collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

The SSH config section is required for this connector.
You'll fill in the database address with a localhost IP address,
and specify your VM's IP address as the SSH address.
See the table below and the [sample config](#sample).

| Property        | Title    | Description                                     | Type    | Required/Default |
|-----------------|----------|-------------------------------------------------|---------|------------------|
| `/database`     | Database | Name of the logical database to materialize to. | string  |                  |
| **`/address`**  | Address  | Host and port. Set to `127.0.0.1:5432` to enable SSH tunneling.                   | string  | Required         |
| **`/password`** | Password | Password for the specified database user.       | string  | Required         |
| `/schema` | Database Schema | Database [schema](https://www.postgresql.org/docs/current/ddl-schemas.html) to use for materialized tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | `"public"` |
| **`/user`**     | User     | Database user to connect as.                    | string  | Required         |
| `networkTunnel` | Network Tunnel | Connect to your system through an SSH server that acts as a bastion host for your network. | Object | |
| `networkTunnel/sshForwarding` | SSH Forwarding | | Object | |
| `networkTunnel/sshForwarding/sshEndpoint` | SSH Endpoint | Endpoint of the remote SSH server (in this case, your Google Cloud VM) that supports tunneling (in the form of ssh://user@address. | String | |
| `networkTunnel/sshForwarding/privateKey` | SSH Private Key | Private key to connect to the remote SSH server. | String | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/additional_table_create_sql` | Additional Table Create SQL | Additional SQL statement(s) to be run in the same transaction that creates the table. | string |  |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |
| `/schema` | Alternative Schema | Alternative schema for this table (optional). Overrides schema set in endpoint configuration. | string |  |
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-alloydb:dev
        config:
          database: postgres
          address: 127.0.0.1:5432
          password: flow
          user: flow
          networkTunnel:
            sshForwarding:
              sshEndpoint: ssh://sshUser@<vm-ip-address>
              privateKey: |2
              -----BEGIN RSA PRIVATE KEY-----
              MIICXAIBAAKBgQCJO7G6R+kv2MMS8Suw21sk2twHg8Vog0fjimEWJEwyAfFM/Toi
              EJ6r5RTaSvN++/+MPWUll7sUdOOBZr6ErLKLHEt7uXxusAzOjMxFKZpEARMcjwHY
              v/tN1A2OYU0qay1DOwknEE0i+/Bvf8lMS7VDjHmwRaBtRed/+iAQHf128QIDAQAB
              AoGAGoOUBP+byAjDN8esv1DCPU6jsDf/Tf//RbEYrOR6bDb/3fYW4zn+zgtGih5t
              CR268+dwwWCdXohu5DNrn8qV/Awk7hWp18mlcNyO0skT84zvippe+juQMK4hDQNi
              ywp8mDvKQwpOuzw6wNEitcGDuACx5U/1JEGGmuIRGx2ST5kCQQDsstfWDcYqbdhr
              5KemOPpu80OtBYzlgpN0iVP/6XW1e5FCRp2ofQKZYXVwu5txKIakjYRruUiiZTza
              QeXRPbp3AkEAlGx6wMe1l9UtAAlkgCFYbuxM+eRD4Gg5qLYFpKNsoINXTnlfDry5
              +1NkuyiQDjzOSPiLZ4Abpf+a+myjOuNL1wJBAOwkdM6aCVT1J9BkW5mrCLY+PgtV
              GT80KTY/d6091fBMKhxL5SheJ4SsRYVFtguL2eA7S5xJSpyxkadRzR0Wj3sCQAvA
              bxO2fE1SRqbbF4cBnOPjd9DNXwZ0miQejWHUwrQO0inXeExNaxhYKQCcnJNUAy1J
              6JfAT/AbxeSQF3iBKK8CQAt5r/LLEM1/8ekGOvBh8MAQpWBW771QzHUN84SiUd/q
              xR9mfItngPwYJ9d/pTO7u9ZUPHEoat8Ave4waB08DsI=
              -----END RSA PRIVATE KEY-----
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

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
