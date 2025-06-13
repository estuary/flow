
# Configure Connections with PrivateLink

If you use Estuary in a [private deployment](../getting-started/deployment-options.md#private-deployment) or [Bring Your Own Cloud](../getting-started/deployment-options.md#byoc-bring-your-own-cloud) setup, you can securely connect services using PrivateLink.
This lets you transfer data between your private or BYOC deployment and external services without exposing it to the public internet.

Different cloud providers have different setup steps to start using PrivateLink.

## AWS PrivateLink

For AWS private or BYOC deployments, we can establish connections to your endpoints using [AWS PrivateLink](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html).

To do so, you will first need to create a Virtual Private Cloud (VPC) endpoint service and associated Network Load Balancer (NLB). The [NLB configuration](#network-load-balancer-setup) will depend on the service you're working with.

You can create or manage your endpoint services on the [AWS VPC dashboard](https://console.aws.amazon.com/vpc/). As you do so:

* Specify the NLB you created.
* Safelist your AWS VPC Account ID (such as `arn:aws:iam::12345:root`) to allow access to your VPC endpoint service.
* Make sure the endpoint service is in the same region as your private deployment.

:::tip
See the [AWS documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/privatelink-share-your-services.html) for the most up-to-date, detailed instructions on working with PrivateLink or contact your AWS representative for assistance.
:::

We will then need the following information to establish the connection. Send these details to your Estuary point of contact:

* The endpoint service name, such as `com.amazonaws.vpce.us-east-1.vpce-svc-0123456789abcdef`
* The Availability Zone IDs it offers, such as `[use1-az4, use1-az6]`

To activate the connection, accept the interface endpoint connection request from Estuary. We will then provide a DNS name which you can use as the hostname when connecting to the database.

### Network Load Balancer Setup

To set up your Virtual Private Cloud endpoint service in AWS, you will first need to [create a Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html) in your VPC. How you configure your NLB will depend on the service you're working with:

* **Static IP**

    If your service is accessed through a single static IP (like EC2, or a non-RDS database), simply ensure that the NLB availability zones match the target availability zones or enable [cross-zone load balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#cross-zone-load-balancing).

* **Dynamic IP**

    If your service has a dynamic IP (such as an Amazon Aurora or RDS database), you will need to configure your Network Load Balancer further to ensure uninterrupted service. There are essentially two different ways to do this.

    * **Use a port forwarding instance**

        Deploy an EC2 instance that is configured to do port forwarding, such as accepting requests from the NLB and forwarding them to your RDS database.

    * **Use the dynamic IP address**

        To work around a database IP that may change without notice, you can deploy a lambda function to periodically check the IP address and update the NLB target group when it changes.

        You can find the IP address by running the `nslookup` or `dig` command with the DNS name for your endpoint. For example:

        ```
        dig +short <YOUR_RDS_DNS_ENDPOINT>
        ```

        Set up your NLB target group with this initial IP address. Your lambda function can then periodically pull the current IP, compare, and update the target group as needed.

### Variations

Certain services may use AWS PrivateLink in unique ways. More detailed instructions for these services are provided below.

**MongoDB**

A MongoDB connection will use an altered endpoint ID to retrieve the hostname.

Create the endpoint service in AWS and receive back the DNS name from Estuary as for a standard AWS PrivateLink connection. To finish setup:

1. Take the first 22 characters of the DNS name, such as `vpce-0123456789abcdefg`.

2. In your MongoDB dashboard, navigate to the **Network Access** section.

3. On the **Private Endpoint** page in this section, add a new private endpoint.

4. In the setup modal, choose your cloud provider and Atlas region. You may skip the "Interface Endpoint" step.

5. As part of the **Finalize Endpoint Connection** step, add the 22-character DNS string as **Your VPC Endpoint ID**.

6. Click **Create** and wait until the Endpoint Status is `Available`.

MongoDB will create a URL for this endpoint. You can use this as the hostname in Estuary to connect with your database.

You can find this URL as you would any other Atlas hostname:

1. Bring up the **Connect** modal for your MongoDB cluster.

2. Make sure to choose **Private Endpoint** as your Connection Type and select the endpoint you created.

3. Choose the **Shell** connection method. The shell command will display your MongoDB URL (for example, `mongodb+srv://abc-123.mongodb.net/`).

## Azure Private Link

For Azure private or BYOC deployments, we can establish connections to your endpoints using Azure Private Link.

You will need to create an [Azure Private Link Service](https://learn.microsoft.com/en-us/azure/private-link/private-link-service-overview) which also requires having an [Azure Load Balancer](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-overview) in front of the services you intend to expose. After creating these resources, make sure your LoadBalancer is able to route traffic correctly to your instances. You can check this by looking at the Monitoring -> Metrics page of your LoadBalancer and checking for its Health Probe Status.

Once you have your Private Link Service set up, we need these details from you to establish the connection. Send them to your Estuary point of contact:

* The service URI, like `/subscriptions/abcdefg-12345-12cc-1234-1234abcd1234abc/resourceGroups/foo/providers/Microsoft.Network/privateLinkServices/bar-service`; this can be found by navigating to the private link service's details page in your Azure Portal and copying the URL
* Location for the private endpoint, like `westus`

After establishing the connection we will give you a private IP address which you can use to connect to your endpoint when setting up your task on the Estuary Flow web app.
