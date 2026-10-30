
This directory contains (older) Kustomize-based deployment
resources for managing Gazette in a Kubernetes environment.

We're actually moving away from Kustomize, and use Tanka ourselves to manage deployments.
(We plan to eventually switch this out with Jsonnet modules for dynamic operations
configuration, plus some generated manifests for quickly spinning up a test cluster).
