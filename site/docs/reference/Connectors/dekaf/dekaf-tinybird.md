# Tinybird

In this guide, you'll learn how to use Estuary Flow to push data streams to Tinybird.

[Tinybird](https://www.tinybird.co/) is a data platform for user-facing analytics.

## Prerequisites

- An Estuary Flow account & collection
- A Tinybird account & Workspace

# Connecting Estuary Flow to Tinybird

1. Create a new access token to use for the Tinybird connection. You can do this from the Estuary Admin Dashboard.
   ![Export Dekaf Access Token](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Group_22_95a85083d4/Group_22_95a85083d4.png)
2. In your Tinybird Workspace, create a new Data Source and use the Kafka Connector.
   ![Configure Estuary Flow Data Source](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Screenshot_2024_08_23_at_15_16_39_35b06dad77/Screenshot_2024_08_23_at_15_16_39_35b06dad77.png)

To configure the connection details, use the following settings.

Bootstrap servers: `dekaf.estuary.dev`
SASL Mechanism: `PLAIN`
SASL Username: `{}`
SASL Password: `Estuary Refresh Token` (Generate your token in the Estuary Admin Dashboard)

Tick the Decode Avro messages with Schema Register box, and use the following settings:

- URL: `https://dekaf.estuary.dev`
- Username: `{}`
- Password: `The same Estuary Refresh Token as above`

![Configure Estuary Flow Schema Registry](https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//Screenshot_2024_08_23_at_15_16_46_374f7f8a12/Screenshot_2024_08_23_at_15_16_46_374f7f8a12.png)

Click Next and you will see a list of topics. These topics are the collections you have in Estuary.
Select the collection you want to ingest into Tinybird, and click Next.

Configure your consumer group as needed.

Finally, you will see a preview of the Data Source schema. Feel free to make any modifications as required, then click
Create Data Source.

This will complete the connection with Tinybird, and new data from the Estuary Flow collection will arrive in your
Tinybird Data Source in real-time.
