Consumer Examples
==================

`Finding Cycles in streaming Citi-Bike Rides`_.
   We've been asked to help with an anomaly detection task: we want to find cases
   where a Citi Bike completes a graph cycle starting and ending at a station **T**,
   without re-visiting **T** in between.

   We'll also offer a "history" API which serves the most recent rides of
   a given bike ID.

`Serving a Real-time Language Model`_.
   We must offer a real-time language model API which accepts new documents to
   add to the corpus, continuously integrates each one, and offers fast query access of
   current model states.

`Summing over Multiplexed File Chunks`_.
   We're presented with a stream of multiplexed file chunks, and must compute
   and publish the full SHA-sum of each file upon it's completion.

   This example also serves as a *soak test* for Gazette and is used to verify
   correctness in automated Jepsen-style fault injection tests.

`Playing Ping-Pong`_.
   Let's play ping pong. *At scale!* This example is a minimal application
   which provides opinionated scaffolding for starting a new Gazette consumer
   project. It demonstrates:

      - Building & end-to-end testing with RocksDB support.
      - Hermetic, Docker-based builds.
      - Protobuf code generation.
      - Packaging release-ready images.
      - Manifests for deploying and testing on Kubernetes.

.. _`Finding Cycles in streaming Citi-Bike Rides`: examples-bike-share.html
.. _`Serving a Real-time Language Model`:          examples-language-model.html
.. _`Summing over Multiplexed File Chunks`:        examples-stream-sum.html
.. _`Playing Ping-Pong`:                           https://github.com/gazette/ping-pong

.. toctree::
   :maxdepth: 2
   :hidden:

   Cycles in Citi-Bike Data   <examples-bike-share>
   Real-time Language Model   <examples-language-model>
   Summing Multiplexed Chunks <examples-stream-sum>

