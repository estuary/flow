## Prerequisites
To use this connector, you'll need :
* An existing catalog spec that includes at least one collection with its schema specified
* A Rockset account with an [API key generated](https://rockset.com/docs/rest-api/#createapikey) from the web UI
* A Rockset workspace
    * Optional; if none exist, one will be created by the connector.
* A Rockset collection
    * Optional; if none exist, one will be created by the connector.

## Configuration
You should have a catalog spec YAML file with, at minimum, one **collection**. You'll add a [materialization](../../../concepts/catalog-entities/materialization.md) that includes the values required by the Rockset connector.

### Values
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
|  |Tenant |String| Required | The tenant in which to create the materialization. This typically matches the collection. |
| |Name | String | Required |The unique name of the materialization |
| `api_key` | API Key | String | Required | Rockset API key generated from the web UI. |
| `workspace` | Workspace | String | **What is default?** | For each binding, name of the Rockset workspace |
| `collection` | Rockset collection | String | **What is default?** | For each binding, the name of the destination Rockset table |
||source | string | Required | For each binding, name of the Flow collection you want to materialize to Rockset. This follows the format `tenant/source_collection` |

:::warning
Check accuracy of above and whether there are additional values
:::

### Sample
Add your materialization to your existing catalog spec YAML file using this example as a model and providing required values per the table above:

```yaml
# If this is the first materialization, add a section to your catalog spec
materializations:
  tenant/mat_name:
	endpoint:
  	  flowSink:
    	    config:
               api_key: supersecret
            # Path to the latest version of the connector, provided as a Docker image
    	    image: ghcr.io/estuary/materialize-rockset:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	workspace: namespace_name
      	collection: table_name
    source: tenant/source_collection
```