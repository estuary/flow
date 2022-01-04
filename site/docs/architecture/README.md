---
title: Architecture
description: How Flow components interact to deploy your pipeline
---

# Architecture

When you use [flowctl](../concepts/flowctl.md) to run your complete [catalog spec](../concepts/#specifications),
the Flow runtime gets to work deploying the catalog and setting your data flow in motion.
The pages in this section discuss how this happens under the hood.

The below diagram roughly describes Flow's architecture for a given data flow:

![](<architecture.svg>)

The Flow runtime includes the activated catalog processes: captures, derivations, and materializations. But note that collections are stored in a cloud data lake, and additional components are involved — collections are composed of **journals,** and **brokers** connect them to the runtime. This architecture allows the low latency and flexible scaling that characterize Flow. You can learn more about it on the [Collection storage](storage.md) page.

Within the runtime, each task — capture, materialization, or derivation — must also be able to scale and use the appropriate compute resources. Flow accomplishes this by using **shards** for processing, and you can optimize it further with **partitions**. To learn more, see the [Runtime processing](scaling.md) page.

:::info
Flow is built on Gazette, an open-source project written and maintained by the creators of Flow. Gazette is important to Flow's architecture — and is the basis for many of Flow's advanced features described in this section — but you don't need to know or use Gazette in order to use Flow.

If you're interested in learning more, you can check out the [Gazette docs](https://gazette.readthedocs.io/en/latest/index.html).
:::


