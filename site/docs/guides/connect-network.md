# Configure connections with SSH tunneling

Depending on your enterprise network security, you may need to use [SHH tunneling](https://www.ssh.com/academy/ssh/tunneling/example#local-forwarding), or port forwarding, to allow Flow
to securely access your endpoint. This is configured within a capture or materialization definition, but
before you can do this, you'll need a properly configured SSH server on your internal network or cloud hosting platform.

This guide includes setup steps for popular cloud platforms,
as well as generalized setup that provide a basic roadmap for on-premise servers or other cloud platforms.

After completing the appropriate setup requirements, proceed to the [configuration](#configuration) section
to add your SSH server to your Flow catalog spec.

## General setup

1. Activate an [SSH implementation on a server](https://www.ssh.com/academy/ssh/server#availability-of-ssh-servers), if you don't have one already.
Consult the documentation for your server's operating system and/or cloud service provider, as the steps will vary.
Configure the server to your organization's standards, or reference the [SSH documentation](https://www.ssh.com/academy/ssh/sshd_config) for
basic configuration options.

2. Referencing the config files and shell output, collect the following information:

   * The **SSH endpoint** for the SSH server, formatted as `ssh://hostname[:port]`. This may look like the any of following:
     * `ssh://ec2-198-21-98-1.compute-1.amazonaws.com`
     * `ssh://198.21.98.1`
     * `ssh://198.21.98.1:22`
   * The SSH **user**, which will be used to log into the SSH server, for example, `sshuser`. You may choose to create a new
  user for this workflow.

3. In the `.ssh` subdirectory of your user home directory,
   look for the PEM file that contains the private SSH key. Check that it starts with `-----BEGIN RSA PRIVATE KEY-----`,
   which indicates it is an RSA-based file.
   * If no such file exists, generate one using the command:
   ```console
      ssh-keygen -m PEM -t rsa
      ```
   * If a PEM file exists, but starts with `-----BEGIN OPENSSH PRIVATE KEY-----`, convert it with the command:
   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key
      ```

  Taken together, these configuration details would allow you to log into the SSH server from your local machine.
  They'll allow the connector to do the same.

5. Configure your internal network to allow the SSH server to access your capture or materialization endpoint.
  Note the internal **host** and **port**; these are necessary to open the connection.

6. Configure your network to expose the SSH server endpoint to eternal traffic. The method you use
   depends on your organization's IT policies. Currently, Estuary doesn't provide a list of static IPs for
   whitelisting purposes, but if you require one, [contact Estuary support](mailto:support@estuary.dev).

7. Choose an open port on your localhost from which you'll connect to the SSH server.

## Setup for AWS

To allow SSH tunneling to a database instance hosted on AWS, you'll need to create a virtual computing environment, or *instance*, in Amazon EC2.

1. Begin by finding your public SSH key on your local machine.
   In the `.ssh` subdirectory of your user home directory,
   look for the PEM file that contains the private SSH key. Check that it starts with `-----BEGIN RSA PRIVATE KEY-----`,
   which indicates it is an RSA-based file.
   * If no such file exists, generate one using the command:
   ```console
      ssh-keygen -m PEM -t rsa
      ```
   * If a PEM file exists, but starts with `-----BEGIN OPENSSH PRIVATE KEY-----`, convert it with the command:
   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key
      ```

2. [Import your SSH key into AWS](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#how-to-generate-your-own-key-and-import-it-to-aws).

3. [Launch a new instance in EC2](https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/LaunchingAndUsingInstances.html). During setup:
   * Configure the security group to allow SSH connection from anywhere.
   * When selecting a key pair, choose the key you just imported.

4. [Connect to the instance](https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/connecting_to_windows_instance.html),
setting the user name to `ec2-user`.

5. Find and note the [instance's public DNS](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-dns.html#vpc-dns-viewing). This will be formatted like: `ec2-198-21-98-1.compute-1.amazonaws.com`.

6. Find and note the host and port for your capture or materialization endpoint.
  :::tip
  For database instances hosted in Amazon RDS, you can find these in the RDS console as Endpoint and Port.
  :::

7. Choose an open port on your localhost from which you'll connect to the SSH server.

## Setup for Google Cloud

To allow SSH tunneling to a database instance hosted on Google Cloud, you must set up a virtual machine (VM).

1. Begin by finding your public SSH key on your local machine.
   In the `.ssh` subdirectory of your user home directory,
   look for the PEM file that contains the private SSH key. Check that it starts with `-----BEGIN RSA PRIVATE KEY-----`,
   which indicates it is an RSA-based file.
   * If no such file exists, generate one using the command:
   ```console
      ssh-keygen -m PEM -t rsa
      ```
   * If a PEM file exists, but starts with `-----BEGIN OPENSSH PRIVATE KEY-----`, convert it with the command:
   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key
      ```
   * If your Google login differs from your local username, add your Gmail or organizational Google email address as a comment:
   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key -C user@domain.com
      ```

2. [Create and start a new VM in GCP](https://cloud.google.com/compute/docs/instances/create-start-instance), [choosing an image that supports OS Login](https://cloud.google.com/compute/docs/images/os-details#user-space-features).

3. [Add your public key to the VM](https://cloud.google.com/compute/docs/connect/add-ssh-keys).

5. [Reserve an external IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address) and connect it to the VM during setup.
Note the generated address.

6. Find and note the host and port for your capture or materialization endpoint.
  :::tip
  For database instances hosted in Google Cloud SQL, you can find the host in the Cloud Console as Public IP Address.
  Use `5432` as the port.
  :::

7. Choose an open port on your localhost from which you'll connect to the SSH server.

## Setup for Azure

To allow SSH tunneling to a database instance hosted on Azure, you'll need to create a virtual machine (VM) in the same virtual network as your endpoint database.

1. Begin by finding your public SSH key on your local machine.
   In the `.ssh` subdirectory of your user home directory,
   look for the PEM file that contains the private SSH key. Check that it starts with `-----BEGIN RSA PRIVATE KEY-----`,
   which indicates it is an RSA-based file.
   * If no such file exists, generate one using the command:
   ```console
      ssh-keygen -m PEM -t rsa
      ```
   * If a PEM file exists, but starts with `-----BEGIN OPENSSH PRIVATE KEY-----`, convert it with the command:
   ```console
      ssh-keygen -p -N "" -m pem -f /path/to/key
      ```

2. Create and connect to a VM in a [virtual network](https://docs.microsoft.com/en-us/azure/virtual-network/virtual-networks-overview), and add the endpoint database to the network.

   1. [Create a new virtual network and subnet](https://docs.microsoft.com/en-us/azure/virtual-network/quick-create-portal).

   2. Create a [Linux](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/quick-create-portal) or [Windows](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/quick-create-portal) VM within the virtual network,
   directing the SSH public key source to the public key you generated previously.

   3. Note the VM's public IP; you'll need this later.

3. Create a service endpoint for your database in the same virtual network as your VM.
Instructions for Azure Database For PostgreSQL can be found [here](https://docs.microsoft.com/en-us/azure/postgresql/howto-manage-vnet-using-portal);
note that instructions for other database engines may be different.

4. Find and note the host and port for your capture or materialization endpoint.
  :::tip
  For database instances hosted in Azure, you can find the host as Server Name, and the port under Connection Strings (usually `5432`).
  :::

5. Choose an open port on your localhost from which you'll connect to the SSH server.



## Configuration

After you've completed the prerequisites, you should have the following parameters:

* `sshEndpoint`: the SSH server's hostname, or public IP address, formatted as `ssh://hostname[:port]`
* `privateKey`: the contents of the PEM file
* `user`: the username used to connect to the SSH server.
* `forwardHost`: the capture or materialization endpoint's host
* `forwardPort`: the capture or materialization endpoint's port
* `localPort`: the port on the localhost used to connect to the SSH server, step 7

1. Use these to add SSH tunneling to your capture or materialization definition, either by filling in the corresponding fields
  in a web app, or by working with the YAML directly. Reference the [Connectors](../../concepts/connectors/#connecting-to-endpoints-on-secure-networks) page for a YAML sample.

  Proxies like SSH are always run on an open port on your localhost, so you'll need to re-configure other fields in your
  capture or materialization definition.

2. Set the host to `localhost`.

3. If the connector has a `port` property, set it to the same value as `localPort` in the SSH configuration.