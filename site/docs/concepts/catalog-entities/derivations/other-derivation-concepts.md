---
description: How selectors and delayed reads are involved in captures
---

# Other derivation concepts

### **Selectors**

Sources can specify a selector over partitions of the sourced collection, which restricts the partitions that are read. Selectors are efficient, as they allow Flow to altogether avoid reading data that’s not needed, rather than performing a read and then filtering it out.

### **Delayed reads**

Event-driven workflows are usually a great fit for reacting to events as they occur, but they aren’t terribly good at taking action when something _hasn’t_ happened:

> * A user adds a product to their cart, but then doesn’t complete a purchase.
> * A temperature sensor stops producing its expected, periodic measurements.

Event-driven solutions to this class of problem are challenging today. Minimally, they require you to integrate another system to manage many timers and tasks, which brings its own issues.

Engineering teams will instead often shrug and switch from an event streaming paradigm to a periodic batch workflow, which is easy to implement but adds irremovable latency.

Flow offers another solution, which is to add an optional **read delay** to a transform. When specified, Flow will use the read delay to **gate** the processing of documents, with respect to the timestamp encoded within each document’s UUID, assigned by Flow at the time the document was ingested or derived. The document will remain gated until the current time is ahead of the document’s timestamp, plus its read delay.

Similarly, if a derivation with a read delay is added later, the delay is also applied to determine the relative processing order of historical documents.

{% hint style="info" %}
Technically, Flow gates the processing of a physical partition, which is very efficient due to Flow’s architecture. Documents that are closely ordered within a partition will also have almost identical timestamps.

For more detail on document UUIDs, see [their Gazette documentation](https://gazette.readthedocs.io/en/latest/architecture-exactly-once.html?#message-uuids).
{% endhint %}

Read delays open up the possibility, for example, of joining a collection _with itself_ to surface cases like shopping cart abandonment or silenced sensors. A derivation might have a real-time transform that updates registers with a “last seen” timestamp on every sensor reading, and another transform with a five-minute delay, that alerts if the “last seen” timestamp hasn’t been updated _since_ that sensor reading.
