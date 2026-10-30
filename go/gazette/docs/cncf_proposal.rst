Gazette Proposal
================

**Name of project:** Gazette

**Description**

Gazette is a broker service providing access to journals (immutable,
ordered, append-only logs). A cluster of Gazette brokers collectively serve
reads and writes across many journals.  Individual brokers are stateless -
journal durability is provided by replicating journals across multiple
brokers (spanning multiple availibility zones). Blob stores provide long-term
durability. Brokers use their local disk only as temporary scratch space for
serving reads and writes. Gazette uses a system of labels and selectors to
define "topics" on an application-specific, ex post facto basis. Producers
and Consumers use selectors to specify the set of journals with which they
are concerned, and can discover new journals that meet their criteria as
they are created. Gazette's Consumer framework supports end-to-end
effectively-once semantics and integrates with a variety of local and
remote systems for storing storing application-defined states materialized
from streams.

**Alignment with CNCF Charter Mission**

Gazette meets the criteria of a cloud native system (container packaged,
dynamically managed and micro-services oriented).  The Gazette community
is looking forward to joining the foundation to help establish neutral
governance and foster the growth of its ecosystem.

**Similar Projects**

*Apache Kafka* is a broker service and framework (KafkaStreams)
for building streaming applications a top a distributed log.  Kafka's
brokers are stateful (primary location for partition data is the
brokers themselves) with support for tiered storage added in recent
versions. Kafka has added limited support for effectively-once semantics
relying on a transactional commit protocol across kafka topics.

*Apache Pulsar* is another broker service, built on top of Apache Bookeeper.
Similar to Gazette, Pulsar keeps data in blob storage. Pulsar provides
out of the box support for effectively once semantics via the Reader consumer
interface, however this only supports non-partitioned topics.

**Sponsor / Advisor from TOC:** TBD

**Preferred Maturity Level:** Sandbox

**License:** MIT

**Source Control:** https://www.github.com/gazette/core

**External Dependencies**

- Golang - Modified BSD https://golang.org/LICENSE
- Google Cloud - Apache 2.0 https://github.com/googleapis/google-cloud-go/blob/master/LICENSE
- ZSTD - Simplified BSD https://github.com/DataDog/zstd/blob/1.x/LICENSE
- AWS SDK - Apache 2.0 https://github.com/aws/aws-sdk-go/blob/master/LICENSE.txt
- Go Humanize - MIT https://github.com/dustin/go-humanize/blob/master/LICENSE
- Golang Petname - Apache 2.0 https://github.com/dustinkirkland/golang-petname/blob/master/LICENSE
- FacebookGo Ensure - BSD https://github.com/facebookarchive/ensure/blob/master/license
- FacebookGo Stack - BSD https://github.com/facebookarchive/stack/blob/master/license
- FacebookGo Subset - BSD https://github.com/facebookarchive/subset/blob/master/license
- Go-Check Check - Simplified BSD https://github.com/go-check/check/blob/v1/LICENSE
- Gogo Protobuf - Modified BSD https://github.com/gogo/protobuf/blob/master/LICENSE
- Golang Protobuf - BSD 3-Clause https://github.com/golang/protobuf/blob/master/LICENSE
- Golang Snappy - BSD 3-Clause https://github.com/golang/snappy/blob/master/LICENSE
- Google UUID - BSD 3-Clause https://github.com/google/uuid/blob/master/LICENSE
- Gorilla Schema - BSD 3-Clause https://github.com/gorilla/schema/blob/master/LICENSE
- Hashicorp Golang LRU - MPL 2.0 https://github.com/hashicorp/golang-lru/blob/master/LICENSE
- Jessevdk Go-Flags - BSD 3-Clause https://github.com/jessevdk/go-flags/blob/master/LICENSE
- Jgraettinger Cockroach Encoding - Apache 2.0 https://github.com/jgraettinger/cockroach-encoding/blob/master/LICENSE
- Jgraettinger Urkel - Apache 2.0 https://github.com/jgraettinger/urkel/blob/master/LICENSE
- Klauspost Compress - BSD 3-Clause https://github.com/klauspost/compress/blob/master/LICENSE
- Klauspost CPUID - MIT https://github.com/klauspost/cpuid/blob/master/LICENSE
- Kr Pretty - MIT https://github.com/kr/pretty/blob/main/License
- Lib Pq - https://github.com/lib/pq/blob/master/LICENSE.md
- Mattn Go-Sqlite3 - MIT https://github.com/mattn/go-sqlite3/blob/master/LICENSE
- Olekukonko Tablewriter - MIT https://github.com/olekukonko/tablewriter/blob/master/LICENSE.md
- Pkg Errors - BSD 2-Clause https://github.com/pkg/errors/blob/master/LICENSE
- Prometheus Client - Apache 2.0 https://github.com/prometheus/client_golang/blob/master/LICENSE
- Prometheus Client Model - Apache 2.0 https://github.com/prometheus/client_model/blob/master/LICENSE
- Sirupsen Logrus - MIT https://github.com/sirupsen/logrus/blob/master/LICENSE
- Soheilhy Cmux - Apache 2.0 https://github.com/soheilhy/cmux/blob/master/LICENSE
- Spf13 Afero - Apache 2.0 https://github.com/spf13/afero/blob/master/LICENSE.txt
- Stretchr Testify - MIT https://github.com/stretchr/testify/blob/master/LICENSE
- Tecbot GoRocksDB - MIT https://github.com/tecbot/gorocksdb/blob/master/LICENSE
- Etcd - Apache 2.0 https://github.com/etcd-io/etcd/blob/master/LICENSE
- Golang X Crypto Modified BSD https://github.com/golang/crypto/blob/master/LICENSE
- Goalng X Net Modified BSD https://github.com/golang/net/blob/master/LICENSE
- Golang X Oauth2 BSD 3-Clause https://github.com/golang/oauth2/blob/master/LICENSE
- Golang X Sync Modified BSD https://github.com/golang/sync/blob/master/LICENSE
- Golang X Sys Modified BSD https://github.com/golang/sync/blob/master/LICENSE
- Golang GRPC - Apache 2.0 https://github.com/grpc/grpc-go/blob/master/LICENSE 
- Alec Thomas Kingping - MIT https://github.com/alecthomas/kingpin/blob/master/COPYING
- Go Yaml - Apache https://github.com/go-yaml/yaml/blob/v2/LICENSE
- Kubernetes API - Apache 2.0 https://github.com/kubernetes/api/blob/master/LICENSE

**Initial Committers:** 

- Johnny Graettinger https://github.com/jgraettinger

**Infrastructure Requests:**

- None?
- AWS Testing
- GCE Testing

**Communication Channels:**

- Slack: gazette-dev

**Issue Tracker:** https://github.com/gazette/core/issues

**Website:** gazette.dev

**Release Methodology and Mechanics:**

Releases follow semver (deliberately pre 1.0 major version).  Currently new
versions are released on an ad-hoc basis.

Release process is roughly:

* Create new major/minor branch and run CI build & test
    * CI builds: container-based build environment for hermetic and reproducible
      builds
    * CI tests: 15 rounds, with race detection
* Package, tag, and push docker images ``gazette/broker`` and ``gazette/examples``


gazette/core repository includes kustomize compatible kubernetes manifests for:

* Deploying brokers
* Base manifests for consumer applications
* Example applications, with bootstrapped cluster dependencies (etcd, minio) for self-contained test environments.
* Jepsen style integration tests, which introduce controlled faults (process stops, crashes, and network partitions) and verify processing semantics and recovery.


**Social Media Accounts:** N/A

**Community Size:**  Small. No existing sponsorship.

**Current User-base:**

- LiveRamp (https://liveramp.com)
    Production deployment scaled to millions of streamed records per second.
- Estuary Tech, Inc (https://estuary.dev)
    Supports Gazette development and is exploring commercialization of products built
    upon it.

**Project Logo**

.. image:: https://gazette.readthedocs.io/en/latest/_images/logo_with_text.svg
