# MySQL HeatWave

This connector lets you materialize data from your Flow collections directly into Oracle MySQL HeatWave instances.

[`ghcr.io/estuary/materialize-mysql-heatwave:dev`](https://github.com/estuary/connectors/pkgs/container/materialize-mysql-heatwave) provides the latest connector image. For earlier versions, please follow the link in your browser.

## Prerequisites
To use this materialization connector, you’ll need the following:

- A MySQL HeatWave database and the appropriate user credentials.
- At least one Flow collection.

## Configuration
Select one or more of your Flow collections to start using this connector. The configuration properties below will help you to materialize your collections into tables in MySQL HeatWave.

## Properties

### Endpoint

| Property                | Title              | Description                                | Type   | Required/Default       |
|-------------------------|--------------------|--------------------------------------------|--------|------------------------|
| **`/address`**         | Address           | Host and port of the database. If only the host is specified, the port will default to `3306`.    | string | Required               |
| **`/database`**         | Database           | Name of the logical database to send data to.  | string | Required               |
| **`/user`**         | User           | Username for authentication.               | string | Required               |
| **`/password`**         | Password           | Password for authentication.               | string | Required               |
| `/timezone`                 | Timezone               | Timezone to use when materializing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read.  | string |                  |

### Advanced: SSL Mode
Configuring the SSL mode strengthens security when transferring data to Oracle MySQL HeatWave. Here are the possible values for SSL mode:

- `disabled`: Establishes an unencrypted connection with the server.
- `preferred`: Initiates the SSL connection only if prompted by the server.
- `required`: Establishes an SSL connection but doesn’t verify the server’s certificate.
- `verify_ca`: Connects via SSL connection and verifies the server’s certificate against the provided SSL Server CA, without validating the server's hostname. SSL Server CA is mandatory for this mode.
- `verify_identity`: Ensures an SSL connection, and verifies both the server's certificate and hostname. This is the highest level of security. SSL Server CA is required for this mode.

### Bindings

| Property                | Title              | Description                                | Type   | Required/Default       |
|-------------------------|--------------------|--------------------------------------------|--------|------------------------|
| **`/table`**            | Table              | The name of the table to send data to.     | string | Required               |



## Sample

```yaml
materializations:
  ${PREFIX}/${MAT_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-mysql-heatwave:dev
        config:
          database: flow
          address: localhost:5432
          password: secret
          user: flow
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## MySQL HeatWave on Managed Cloud Platforms

This connector supports cloud-based MySQL HeatWave instances. For secure connections, it's highly recommended to use an SSH tunnel.

Supported platforms include Google Cloud Platform, Amazon Web Service, and Microsoft Azure. Other cloud platforms may also be compatible, but Estuary doesn't guarantee performance.

### Setup
To allow connections from Estuary, it's recommended that you whitelist Estuary Flow's IP address.

- **Amazon RDS and Amazon Aurora:** Edit the VPC security group associated with your database instance or create a new VPC security group and link it to the database instance. Set the instance to Publicly accessible under Connectivity settings. Create new inbound and outbound rules to allow all traffic from IP address 34.121.207.128 following the steps in [Amazon's documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html#Overview.RDSSecurityGroups.Create).

- **Google Cloud SQL:** Activate public IP on your database and authorize the IP address 34.121.207.128.

- **Azure Database for MySQL:** Create a new firewall rule granting access to IP address 34.121.207.128.

#### SSH Tunneling
You can also allow secure connections using SSH tunneling settings:

- **SSH Endpoint**: Enter the endpoint of the remote SSH server that supports tunneling (formatted as `ssh://user@hostname[:port]`).
- **SSH Private Key**: Input the full RSA Private Key for SSH connection.
