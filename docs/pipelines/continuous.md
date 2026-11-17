---
title: Run a continuous pipeline
description: Configure a pipeline that keeps reading and commits data at intervals.
---

A continuous pipeline keeps reading changes instead of stopping after a scheduled sync.

Before you start, configure a source and destination. The source must support CDC or stream reads.

## Continuous pipelines

A continuous pipeline has `mode: continuous` and no `schedule`. Its job keeps reading and commits at intervals. The default is 10 seconds unless the destination or `delivery.commit.interval` specifies another value:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: orders-live
spec:
  mode: continuous
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: cdc
  destination: { ref: warehouse, namespace: shop }
```

Its streams must be read in a way that never ends: `cdc` from a database, or `stream` from an event stream such as a topic. The plan fails a stream read any other way. A continuous pipeline has no syncs, so it has no `postSync` steps and no Spark transform. Pausing it stops its job; resuming starts it again from its last checkpoint.
