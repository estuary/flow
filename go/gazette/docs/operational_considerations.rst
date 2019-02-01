Operational considerations
==========================

This document offers some recommendations based on lessons learned from running 
Gazette in production at LiveRamp.

Cloud Costs
~~~~~~~~~~~

At LiveRamp, Gazette is deployed entirely in the cloud: both the write clients
and the Gazette brokers run on a Kubernetes cluster whose underlying nodes are
spread across multiple zones, and journal content is persisted to cloud storage.
While relying on cloud infrastructure makes it easy to scale usage up and down as
needed, it can also make the task of predicting costs more difficult, especially
for high-traffic systems that deal with data on the order of terabytes each month.

Storage Costs
`````````````

1. For non-recovery log journals, define retention policies for each category of 
   data according to its usage requirements, and then enforce these by configuring
   lifecycle policies via the cloud storage provider. For example, one could have
   a bucket's files be transitioned to cold storage X days after creation (which 
   would keep the data backed up but not immediately accessible for a much lower 
   cost than standard storage), and then finally have it transition to expiry. 
2. For recovery log journals, use the `prune-log` command-line tool periodically 
   to delete fragments that are no longer needed. Note that a simple time-based
   lifecycle policy like the above will not work for recovery logs. 

Data Transfer Costs
```````````````````

If write clients and Gazette brokers are spread across multiple availability zones
or even regions, consider the costs incurred by inter-zone traffic. A client could
issue a write to a broker in a different zone, and that data might be replicated
to multiple distinct brokers (depending on the replication factor) in other zones.
This meants that a write of size X might lead to inter-zone traffic of more than 
2X in the worst case, and these costs can add up very quickly when billions of
writes are involved each day.

While there is not currently a way to mitigate data transfer costs due to writes,
data transfer costs due to reads can be addressed on a Kubernetes deployment 
via the mechanism of a `gazette-zonemap` ConfigMap: 
https://github.com/LiveRamp/gazette/blob/master/v2/charts/consumer/templates/deployment.yaml#L55.
This ensures that a read from a client will go to a broker in the same zone, if
there is one available.

