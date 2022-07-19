# Configure connections with SSH tunneling

Flow connects to certain types of endpoints — generally databases — using their IP address and port.
For added security, you can configure [SSH tunneling](https://www.ssh.com/academy/ssh/tunneling/example#local-forwarding), also known as port forwarding.
You configure this in the `networkTunnel` section of applicable capture or materialization definitions, but
before you can do so, you need a properly configured SSH server on your internal network or cloud hosting platform.

This guide includes setup steps for popular cloud platforms,
as well as generalized setup that provides a basic roadmap for on-premise servers or other cloud platforms.

After completing the appropriate setup requirements, proceed to the [configuration](#configuration) section
to add your SSH server to your capture or materialization definition.

## General setup

1. Activate an [SSH implementation on a server](https://www.ssh.com/academy/ssh/server#availability-of-ssh-servers), if you don't have one already.
Consult the documentation for your server's operating system and/or cloud service provider, as the steps will vary.
Configure the server to your organization's standards, or reference the [SSH documentation](https://www.ssh.com/academy/ssh/sshd_config) for
basic configuration options.

2. Referencing the config files and shell output, collect the following information:

  * The SSH **user**, which will be used to log into the SSH server, for example, `sshuser`. You may choose to create a new
  user for this workflow.
  * The **SSH endpoint** for the SSH server, formatted as `ssh://user@hostname[:port]`. This may look like the any of following:
     * `ssh://sshuser@ec2-198-21-98-1.compute-1.amazonaws.com`
     * `ssh://sshuser@198.21.98.1`
     * `ssh://sshuser@198.21.98.1:22`
     :::info Hint
     The [SSH default port is 22](https://www.ssh.com/academy/ssh/port).
     Depending on where your server is hosted, you may not be required to specify a port,
     but we recommend specifying `:22` in all cases to ensure a connection can be made.
     :::


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

6. Configure your network to expose the SSH server endpoint to external traffic. The method you use
   depends on your organization's IT policies. Currently, Estuary doesn't provide a list of static IPs for
   whitelisting purposes, but if you require one, [contact Estuary support](mailto:support@estuary.dev).

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

3. [Launch a new instance in EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html). During setup:
   * Configure the security group to allow SSH connection from anywhere.
   * When selecting a key pair, choose the key you just imported.

4. [Connect to the instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstances.html),
setting the user name to `ec2-user`.

5. Find and note the [instance's public DNS](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-dns.html#vpc-dns-viewing). This will be formatted like: `ec2-198-21-98-1.compute-1.amazonaws.com`.

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
   * If your Google login differs from your local username, generate a key that includes your Google email address as a comment:
   ```console
      ssh-keygen -m PEM -t rsa -C user@domain.com
      ```

2. [Create and start a new VM in GCP](https://cloud.google.com/compute/docs/instances/create-start-instance), [choosing an image that supports OS Login](https://cloud.google.com/compute/docs/images/os-details#user-space-features).

3. [Add your public key to the VM](https://cloud.google.com/compute/docs/connect/add-ssh-keys).

5. [Reserve an external IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address) and connect it to the VM during setup.
Note the generated address.

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

## Configuration

After you've completed the prerequisites, you should have the following parameters:

* **SSH Endpoint** / `sshEndpoint`: the remote SSH server's hostname, or public IP address, formatted as `ssh://user@hostname[:port]`

     The [SSH default port is 22](https://www.ssh.com/academy/ssh/port).
     Depending on where your server is hosted, you may not be required to specify a port,
     but we recommend specifying `:22` in all cases to ensure a connection can be made.

* **Private Key** / `privateKey`: the contents of the SSH private key file

Use these to add SSH tunneling to your capture or materialization definition, either by filling in the corresponding fields
in the web app, or by working with the YAML directly. Reference the [Connectors](../../concepts/connectors/#connecting-to-endpoints-on-secure-networks) page for a YAML sample.
