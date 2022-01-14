# Configure connections with SSH tunneling

Depending on your enterprise network security, you may need to use [SHH tunneling](https://www.ssh.com/academy/ssh/tunneling/example#local-forwarding), or port forwarding, to allow Flow
to securely access your endpoint. This is configured within a capture or materialization definition, but
before you can do this, you'll need a properly configured SSH server on your internal network.

These steps provide a basic roadmap, and should be followed with the help of an IT specialist in your organization,
as details depend on your organization's policies and practices.

1. Activate an [SSH implementation on a server](https://www.ssh.com/academy/ssh/server#availability-of-ssh-servers), if you don't have one already.
Consult the documentation for your server's operating system and/or cloud service provider, as the steps will vary.
Configure the server to your organization's standards, or reference the [SSH documentation](https://www.ssh.com/academy/ssh/sshd_config) for
basic configuration options.

2. Referencing the config files and shell output, collect the following information:

   * The **endpoint** for the SSH server, which may look like the any of following:
     * `ec2-198-21-98-1.compute-1.amazonaws.com`
     * `198.21.98.1`
     * `198.21.98.1:22`
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

4. The PEM files must be encoded with Base64 before they are passed to a connector.
   You can convert it with the following bash command:
      ``` console
      cat <path-to-the-pem-file> | base64 -w 0
      ```

5. Configure your internal network to allow the SSH server to access the endpoint.
  Note the internal **host** and **port**; these are necessary to open the connection.

6. Configure your network to expose the SSH server endpoint to eternal traffic. The method you use
   depends on your organization's IT policies. Currently, Estuary doesn't provide a list of static IPs for
   whitelisting purposes, but if you require one, [contact Estuary support](mailto:support@estuary.dev).

7. You can also choose a port on your localhost to use to connect to the SSH server. If you don't have a
   reason to pick a particular point, it is recommended to set this value to `0` in the configuration.
   This will allow Flow to dynamically pick an open port.

### Configuration

After you've completed the prerequisites, you should have the following parameters:

* `ssh_endpoint`: the endpoint, step 2
* `ssh_private_key_base64`: the encoded PEM file, step 4
* `ssh_user`: the username, step 2
* `remote_host`: the endpoint's host, step 5
* `remote_port`: the endpoint's port, step 5
* `local_port`: the port on the localhost used to connect to the SSH server, step 7

1. Use these to add SSH tunneling to your capture or materialization definition, either by filling in the corresponding fields
  in a web app, or by working with the YAML directly. Reference the [Connectors](../../concepts/connectors/#connecting-to-endpoints-on-secure-networks) page for a code sample.

Proxies like SSH are always run on an open port on your localhost, so you'll need to re-configure other fields in your
capture or materialization definition.

2. Set the host to `localhost`.

3. Set the port to the same value you chose for `local_port` in the SSH configuration.