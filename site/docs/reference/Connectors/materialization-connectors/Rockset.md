This Flow connector materializes [delta updates](../../../concepts/catalog-entities/materialization.md#how-materializations-work-) of your Flow collections into Rockset collections.

`ghcr.io/estuary/materialize-rockset:dev` provides the latest connector image when using the Flow GitOps environment. You can also follow the link in your browser to see past image versions.

## Prerequisites
To use this connector, you'll need :
* An existing catalog spec that includes at least one collection with its schema specified
* A Rockset account with an [API key generated](https://rockset.com/docs/rest-api/#createapikey) from the web UI
* A Rockset workspace
    * Optional; if none exist, one will be created by the connector.
* A Rockset collection
    * Optional; if none exist, one will be created by the connector.

## Configuration
To use this connector, begin with a Flow catalog that has at least one **collection**. You'll add a Rockset materialization, which will direct one or more of your Flow collections to your desired Rockset collections. Follow the basic [materialization setup](../../../concepts/catalog-entities/materialization.md) and add the required Rockset configuration values per the table below.

### Values
| Value | Name | Type | Required/Default | Details |
|-------|------|------|---------| --------|
| `api_key` | API Key | String | Required | Rockset API key generated from the web UI. |
| `HttpLogging` | HTTP Logging | bool | false | Enable verbose logging of the HTTP calls to the Rockset API |
| `MaxConcurrentRequests` | Maximum Concurrent Requests | int | 1 | The upper limit on how many concurrent requests will be sent to Rockset. |
| `workspace` | Workspace | String | Required | For each binding, name of the Rockset workspace |
| `collection` | Rockset collection | String | Required| For each binding, the name of the destination Rockset table |

### Sample

```yaml
# If this is the first materialization, add the section to your catalog spec
materializations:
  ${tenant}/${mat_name}:
	  connector:
  	  flowSink:
    	    config:
               api_key: supersecret
            # Path to the latest version of the connector, provided as a Docker image
    	    image: ghcr.io/estuary/materialize-rockset:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	workspace: ${namespace_name}
      	collection: ${table_name}
    source: ${tenant}/${source_collection}
```