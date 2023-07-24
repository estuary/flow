---
sidebar_position: 1
---
# OracleDB
This connector captures data from OracleDB into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, ghcr.io/estuary/source-oracle:dev provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
* Oracle 11g or above
* Allow connections from Estuary Flow to your Oracle database (if they exist in separate VPCs)
* Create a dedicated read-only Estuary Flow user with access to all tables needed for replication

## Setup
Follow the steps below to set up the OracleDB connector.

### Set up the OracleDB connector in Estuary Flow

1. Log into your Estuary Flow account.
2. In the left navigation bar, click on "Captures". In the top-left corner, click "Connector Search".
3. Enter the name for the OracleDB connector and select "Oracle Database" from the dropdown.
4. Enter a Primary Key using the standard form editor.
5. Add Schemas: JDBC URL Params. Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs separated by the symbol '&'. (example: key1=value1&key2=value2&key3=value3).
6. Choose an option to Connect By:
* Service Name
* System ID
7. On the next page, select your Cursor Field.  This currently has to be either a string or Timestamp value.

### Create a Dedicated User

Creating a dedicated database user with read-only access is recommended for better permission control and auditing.

1. To create the user, run the following commands against your database:

```sql
CREATE USER estuary_flow_user IDENTIFIED BY <your_password_here>;
GRANT CREATE SESSION TO estuary_flow_user;
```

2. Next, grant the user read-only access to the relevant tables. The simplest way is to grant read access to all tables in the schema as follows:

```sql
GRANT SELECT ANY TABLE TO estuary_flow_user;
```

3. Alternatively, you can be more granular and grant access to specific tables in different schemas:

```sql
GRANT SELECT ON "<schema_a>"."<table_1>" TO estuary_flow_user;
GRANT SELECT ON "<schema_b>"."<table_2>" TO estuary_flow_user;
```

4. Your database user should now be ready for use with Estuary Flow.

### Include Schemas for Discovery
In your Oracle configuration, you can specify the schemas that Flow should look at when discovering tables. The schema names are case-sensitive and will default to the upper-cased user if empty. If the user does not have access to the configured schemas, no tables will be discovered.

### SSH Tunnel Configuration
If your Oracle instance is not directly accessible and you need to connect via an SSH tunnel, follow these additional steps.

1. Choose the SSH Tunnel Method:
* No Tunnel (default) for a direct connection.
* SSH Key Authentication or Password Authentication for SSH tunneling.

2. SSH Tunnel Jump Server Host: Provide the hostname or IP Address of the intermediate (bastion) server that Estuary Flow will connect to.

3. SSH Connection Port: Set the port on the bastion server with which to make the SSH connection. The default port for SSH connections is 22.

4. SSH Login Username: The username that Estuary Flow should use when connecting to the bastion server. This is NOT the Oracle username.
* For Password Authentication: Set SSH Login Username to the password of the user created in Step 2.
* For SSH Key Authentication: Leave SSH Login Username blank.

5. SSH Private Key (for SSH Key Authentication): Provide the RSA Private Key that you are using to create the SSH connection. The key should be in PEM format, starting with -----BEGIN RSA PRIVATE KEY----- and ending with -----END RSA PRIVATE KEY-----.

### Encryption Options
Estuary Flow has the ability to connect to the Oracle source with 3 network connectivity options:

1. Unencrypted: The connection will be made using the TCP protocol, transmitting all data over the network in unencrypted form.

2. Native Network Encryption: Gives you the ability to encrypt database connections, without the configuration overhead of TCP/IP and SSL/TLS and without the need to open and listen on different ports. In this case, the SQLNET.ENCRYPTION_CLIENT option will always be set as REQUIRED by default. The client or server will only accept encrypted traffic, but the user has the opportunity to choose an Encryption algorithm according to the security policies they need.

3. TLS Encrypted (verify certificate): If this option is selected, data transfer will be transmitted using the TLS protocol, taking into account the handshake procedure and certificate verification. To use this option, insert the content of the certificate issued by the server into the SSL PEM file field.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the OracleDB source connector.

### Properties

#### Endpoint
| Property    | Title    | Description                                                                                                                                                                                                                                                                     | Type   | Required/Default |
| ----------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| `/host`     | Host     | Hostname of the database.                                                                                                                                                                                                                                                       | string | Required         |
| `/port`     | Port     | Port of the database. Oracle Corporations recommends the following port numbers: 1521 - Default listening port for client connections to the listener. 2484 - Recommended and officially registered listening port for client connections to the listener using TCP/IP with SSL | string | Required         |
| `/user`     | User     | The username which is used to access the database.                                                                                                                                                                                                                              | string | Required         |
| `/password` | Password | The password associated with the username                                                                                                                                                                                                                                       | string | Required         |


#### Bindings

| Property        | Title     | Description                                                            | Type   | Required/Default |
| --------------- | --------- | ---------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your OracleDB project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                     | string | Required         |


### Sample

```json
{
  "required": [
    "host",
    "port",
    "username",
    "connection_data",
    "encryption"
  ],
  "properties": {
    "connection_data": {
      "default": {
        "connection_type": "service_name"
      },
      "discriminator": {
        "propertyName": "connection_type"
      },
      "oneOf": [
        {
          "description": "Use service name",
          "properties": {
            "connection_type": {
              "const": "service_name",
              "order": 0,
              "type": "string",
              "default": "service_name"
            },
            "service_name": {
              "order": 1,
              "title": "Service name",
              "type": "string"
            }
          },
          "required": [
            "service_name"
          ],
          "title": "Service name"
        },
        {
          "description": "Use SID (Oracle System Identifier)",
          "properties": {
            "connection_type": {
              "const": "sid",
              "order": 0,
              "type": "string",
              "default": "sid"
            },
            "sid": {
              "order": 1,
              "title": "System ID (SID)",
              "type": "string"
            }
          },
          "required": [
            "sid"
          ],
          "title": "System ID (SID)"
        }
      ]
    },
    "encryption": {
      "default": {
        "encryption_method": "unencrypted"
      },
      "discriminator": {
        "propertyName": "encryption_method"
      },
      "oneOf": [
        {
          "description": "Data transfer will not be encrypted.",
          "properties": {
            "encryption_method": {
              "const": "unencrypted",
              "type": "string",
              "default": "unencrypted"
            }
          },
          "required": [
            "encryption_method"
          ],
          "title": "Unencrypted"
        },
        {
          "description": "The native network encryption gives you the ability to encrypt database connections, without the configuration overhead of TCP/IP and SSL/TLS and without the need to open and listen on different ports.",
          "properties": {
            "encryption_algorithm": {
              "default": "AES256",
              "description": "This parameter defines what encryption algorithm is used.",
              "enum": [
                "AES256",
                "RC4_56",
                "3DES168"
              ],
              "title": "Encryption Algorithm",
              "type": "string"
            },
            "encryption_method": {
              "const": "client_nne",
              "type": "string",
              "default": "client_nne"
            }
          },
          "required": [
            "encryption_method"
          ],
          "title": "Native Network Encryption (NNE)"
        },
        {
          "description": "Verify and use the certificate provided by the server.",
          "properties": {
            "encryption_method": {
              "const": "encrypted_verify_certificate",
              "type": "string",
              "default": "encrypted_verify_certificate"
            },
            "ssl_certificate": {
              "airbyte_secret": true,
              "description": "Privacy Enhanced Mail (PEM) files are concatenated certificate containers frequently used in certificate installations.",
              "multiline": true,
              "order": 4,
              "title": "SSL PEM File",
              "type": "string"
            }
          },
          "required": [
            "encryption_method",
            "ssl_certificate"
          ],
          "title": "TLS Encrypted (verify certificate)"
        }
      ]
    },
    "port": {
      "description": "Port of the database. Oracle Corporations recommends the following port numbers: 1521 - Default listening port for client connections to the listener. 2484 - Recommended and officially registered listening port for client connections to the listener using TCP/IP with SSL"
    },
    "tunnel_method": {
      "default": {
        "tunnel_method": "NO_TUNNEL"
      },
      "discriminator": {
        "propertyName": "tunnel_method"
      },
      "oneOf": [
        {
          "properties": {
            "tunnel_method": {
              "const": "NO_TUNNEL",
              "description": "No ssh tunnel needed to connect to database",
              "order": 0,
              "type": "string",
              "default": "NO_TUNNEL"
            }
          },
          "required": [
            "tunnel_method"
          ],
          "title": "No Tunnel"
        },
        {
          "properties": {
            "ssh_key": {
              "airbyte_secret": true,
              "description": "OS-level user account ssh key credentials in RSA PEM format ( created with ssh-keygen -t rsa -m PEM -f myuser_rsa )",
              "multiline": true,
              "order": 4,
              "title": "SSH Private Key",
              "type": "string"
            },
            "tunnel_host": {
              "description": "Hostname of the jump server host that allows inbound ssh tunnel.",
              "order": 1,
              "title": "SSH Tunnel Jump Server Host",
              "type": "string"
            },
            "tunnel_method": {
              "const": "SSH_KEY_AUTH",
              "description": "Connect through a jump server tunnel host using username and ssh key",
              "order": 0,
              "type": "string",
              "default": "SSH_KEY_AUTH"
            },
            "tunnel_port": {
              "default": 22,
              "description": "Port on the proxy/jump server that accepts inbound ssh connections.",
              "examples": [
                "22"
              ],
              "maximum": 65536,
              "minimum": 0,
              "order": 2,
              "title": "SSH Connection Port",
              "type": "integer"
            },
            "tunnel_user": {
              "description": "OS-level username for logging into the jump server host.",
              "order": 3,
              "title": "SSH Login Username",
              "type": "string"
            }
          },
          "required": [
            "tunnel_method",
            "tunnel_host",
            "tunnel_port",
            "tunnel_user",
            "ssh_key"
          ],
          "title": "SSH Key Authentication"
        },
        {
          "properties": {
            "tunnel_host": {
              "description": "Hostname of the jump server host that allows inbound ssh tunnel.",
              "order": 1,
              "title": "SSH Tunnel Jump Server Host",
              "type": "string"
            },
            "tunnel_method": {
              "const": "SSH_PASSWORD_AUTH",
              "description": "Connect through a jump server tunnel host using username and password authentication",
              "order": 0,
              "type": "string",
              "default": "SSH_PASSWORD_AUTH"
            },
            "tunnel_port": {
              "default": 22,
              "description": "Port on the proxy/jump server that accepts inbound ssh connections.",
              "examples": [
                "22"
              ],
              "maximum": 65536,
              "minimum": 0,
              "order": 2,
              "title": "SSH Connection Port",
              "type": "integer"
            },
            "tunnel_user": {
              "description": "OS-level username for logging into the jump server host",
              "order": 3,
              "title": "SSH Login Username",
              "type": "string"
            },
            "tunnel_user_password": {
              "airbyte_secret": true,
              "description": "OS-level password for logging into the jump server host",
              "order": 4,
              "title": "Password",
              "type": "string"
            }
          },
          "required": [
            "tunnel_method",
            "tunnel_host",
            "tunnel_port",
            "tunnel_user",
            "tunnel_user_password"
          ],
          "title": "Password Authentication"
        }
      ]
    }
  }
}
```
