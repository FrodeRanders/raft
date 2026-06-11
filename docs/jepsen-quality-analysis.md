# Thoughts around the Jepsen test suite

I observed a problem with the 'snapshot-boundary-restart' Jepsen testcase.

```
INFO [2026-06-07 16:16:54,717] jepsen test runner - jepsen.core {:linearizable {:valid? true,
                :configs ({:model #knossos.model.CASRegister{:value "v0"},
                           :last-op {:process 0,
                                     :type :ok,
                                     :f :read,
                                     :value "v0",
                                     :index 220,
                                     :time 104748472042,
                                     :raw-response {:peerId "n3",
                                                    :leaderPort 20462,
                                                    :leaderId "n3",
                                                    :success true,
                                                    :status "OK",
                                                    :result {:type "GET",
                                                             :key "k",
                                                             :found true,
                                                             :value "v0"},
                                                    :leaderHost "127.0.0.1",
                                                    :message "Query completed"},
                                     :stderr ""},
                           :pending []}),
                :final-paths ()},
 :nemesis-errors {:valid? false,
                  :errors [#jepsen.history.Op{:index 201,
                                              :time 95349690917,
                                              :type :info,
                                              :process :nemesis,
                                              :f :start,
                                              :value nil,
                                              :error :snapshot-boundary-timeout}]},
 :timeline {:valid? true},
 :valid? false}
 ```

This run did not expose a Raft safety failure. It exposed a failing Jepsen/nemesis precondition in 
snapshot-boundary-restart: the nemesis never observed a compacted follower, timed out, and the 
nemesis-errors checker correctly marked the test invalid.

The actual linearizability checker reported `:valid? true` for the failing case. The suite failed only 
because the nemesis emitted:
```
:error :snapshot-boundary-timeout
```
and the composed checker treats nemesis errors as test failures.

## What happened in the failing test?

It was run with:
```
--time-limit 60
--concurrency 10
--node-count 5
--nemesis snapshot-boundary-restart
--nemesis-interval 5
--snapshot-min-entries 3
--snapshot-chunk-bytes 1024
```

The important timeline in the log is:

```
16:15:59.685  worker 2 :ok :cas ["v4" "v3"]
16:16:00.331  worker 6 :ok :write "v0"
16:16:00.393  worker 4 :ok :read "v0"
...
16:16:42.700  nemesis :info :start nil :snapshot-boundary-timeout
...
16:16:54.717  analysis:
  :linearizable {:valid? true ...}
  :nemesis-errors {:valid? false, :errors [... :snapshot-boundary-timeout ...]}
  :valid? false
```

So the workload history itself was linearizable. The system ended in a stable value `"v0"`, and all final reads 
returned `"v0"`. The failure is that the snapshot-boundary nemesis was unable to find a follower with 
`snapshotIndex > 0`.

The harness code confirms that snapshot-boundary-restart is deliberately waiting for a real compaction boundary 
before restarting a node. It polls telemetry and requires a non-leader node with positive snapshotIndex; if none 
appears within 90 seconds, it returns `:snapshot-boundary-timeout.`

That is a rational test design: it avoids pretending to test snapshot recovery when no snapshot has actually occurred.

## Is the failure likely a timing problem?

Yes, but specifically a test-trigger timing / observability problem, not yet a demonstrated Raft correctness problem.

The Apple Silicon run appears to have completed enough client operations to satisfy “probably should snapshot soon” 
from the outside:

```
snapshot-boundary-restart:
  111 invokes
  83 ok
  27 CAS mismatches
  1 nemesis timeout
```

But the nemesis did not observe a compacted follower. Possible causes:

1. Snapshotting is asynchronous and did not complete before the nemesis timeout.
2. Only the leader compacted, while the nemesis requires a non-leader node.
3. Telemetry did not expose snapshotIndex consistently or returned stale/zero data.
4. The snapshot threshold was configured, but not actually applied to the Java node processes.
5. Writes were acknowledged before enough entries were applied locally on followers to trigger follower-side snapshotting.
6. A follower had compacted, but cluster-summary / telemetry-detail failed or was filtered out because :success was false.

The last two are especially plausible. The nemesis does this, conceptually:

```
leader = leader-node(test)
for candidate in shuffled nodes:
   summary = telemetry-detail(test, candidate)
   snapshotIndex = summary.snapshotIndex or 0
   accept candidate only if:
     summary.success
     snapshotIndex > 0
     candidate != leader
```

So a healthy leader with a snapshot is not enough; a follower must expose it.

## Are the accepted tests necessarily “correct”?

Mostly yes, with some important caveats.

The good parts are:

The workload uses Jepsen’s CAS-register model: reads, writes, and CAS operations are appropriate for testing 
single-key linearizability. Jepsen’s linearizability checker uses Knossos; Knossos reports true when a history 
is linearizable, false when it is not, and `:unknown` when analysis cannot complete.

The checker composition is sensible. It checks linearizability and separately checks nemesis failures. 
That separation is important because linearizability checkers normally ignore nemesis/control-plane events. 
The nemesis-error-checker makes sure a test does not pass merely because the fault injector silently failed.

The client classification is also mostly well-formed. Definite successes become `:ok`; definite CAS mismatches 
become `:fail`; unreachable/timeouts become `:info`, which is the right Jepsen shape for uncertain operations. 
The client code explicitly documents this distinction.

The final read phase is useful. Jepsen records operation starts and completions in a history while a nemesis 
injects faults, and final reads help expose lost updates or stale state after the system has settled.

## The main weakness in the accepted tests

The accepted tests are rational, but they are still relatively weak as evidence.

Most cases execute only tens of successful operations. For example, the baseline had about 30 successful 
operations; many fault cases had 30–45 successful operations. That is enough for a smoke/regression suite, 
but not enough to give high confidence in a Raft implementation.

Also, the value domain is tiny:

```
["v0" "v1" "v2" "v3" "v4"]
```

This is convenient, but repeated writes of the same values make many histories easier to linearize than 
they should be. A stale read of `"v1"` is harder to detect if `"v1"` occurred repeatedly in multiple plausible 
places. For linearizability testing, unique or monotonically tagged values give the checker much more 
discriminating power.

For example, instead of random values from five strings, use values like:
```
w7-v0042
w3-v0017
```
or, even better, structured values:
```
{:process 7 :op 42 :uuid "..."}
```

That makes a returned value identify a specific write.

## One specific issue: bounded generator vs time-limited test

The workload generator applies gen/limit 100, and then the full generator wraps it in gen/time-limit.
The workload generator has gen/limit 100, and the full generator then uses gen/time-limit around the 
client/nemesis mix.

This means some runs may become operation-count limited before they become time limited. In the failing 
snapshot case, the client workload appears to be finished around `16:16:00`, while the nemesis only returns 
its timeout at `16:16:42`.

That matters for snapshot testing. If the goal is “run until compaction has definitely happened, then restart 
at the boundary”, the generator should not rely on a small probabilistic operation mix to create enough applied 
entries. For snapshot-boundary testing, the prelude should be deterministic.

## Recommended changes

### 1. Split snapshot-boundary-restart into a deterministic prelude and a nemesis

Instead of hoping the mixed workload triggers compaction, explicitly drive enough writes first.

Conceptually:

```
(gen/phases
  ;; deterministic snapshot warm-up
  (gen/clients
    (gen/each-thread
      (gen/seq
        (map write-specific-values (range 0 50)))))

  ;; wait until at least one follower reports snapshotIndex > 0
  (gen/nemesis (gen/once {:f :start}))

  ;; restart selected compacted follower

  ;; continue concurrent workload
  (gen/time-limit 30 normal-client-load)

  ;; final reads
  ...)
```

Even better: make the nemesis itself report the observed telemetry samples during timeout. Right now 
the failure says only “snapshot-boundary-timeout”. We want to know whether it saw:

```
n1 success=true  snapshotIndex=0 leader=n3
n2 success=false ...
n3 success=true  snapshotIndex=12 leader=n3
...
```
That would immediately distinguish “no snapshots occurred” from “only leader snapshotted” from 
“telemetry unavailable”.

### 2. Make timeout diagnostics first-class

Change the timeout return from:
```
:error :snapshot-boundary-timeout
```

to something like:

```
:error :snapshot-boundary-timeout
:value {:last-observed {...}
        :samples [...]
        :required {:success true
                   :snapshotIndex "> 0"
                   :notLeader true}}
```

This preserves good nemesis-errors behavior but makes the failure actionable.

### 3. Increase entropy of write values

Replace the five reusable values with unique generated values. This gives Knossos much stronger 
evidence and makes accepted tests more meaningful.

Current random values are fine for smoke tests, but for extended Jepsen runs we should use unique values.

### 4. Separate smoke, regression, and stress profiles

The current suite suffices as a local smoke/regression suite, but we want a heavier profile:

```
--time-limit 120
--concurrency 20 or 50
--operation-limit 5000 or unbounded by count
--unique-values true
```

For day-to-day development:

```
smoke:      10–20 s, small values, quick feedback
regression: 60–120 s, unique values, all nemeses
stress:     10–60 min, high concurrency, repeated seeds
```

### 5. Add seed reporting

If not already captured in the store metadata, print and persist the random seed. For timing-sensitive 
Apple-vs-Intel differences, reproducibility matters.

### 6. Treat `snapshot-partition-leader` as weaker than its name suggests

The accepted snapshot-partition-leader case passed linearizability, but from the log alone we cannot 
see evidence that a snapshot actually occurred. It was configured with snapshot parameters, but unlike 
snapshot-boundary-restart, it does not appear to require observing `snapshotIndex > 0`.

So we should interpret:
```
snapshot-partition-leader: PASS
```
as:
```
Linearizability survived a leader partition while low snapshot thresholds were configured.
```
not as:
```
Snapshot installation/compaction definitely interacted with leader partition and recovered correctly.
```

For a true snapshot test, require an observed snapshot boundary.

## Conclusion

The test harness is fundamentally rational. In particular:
```
linearizability valid  +  nemesis timeout invalid  => overall FAIL
```
is exactly the right behavior.

But the failing case is not yet evidence of a Raft safety violation. It is evidence that the 
snapshot-boundary-restart scenario did not successfully establish its own precondition on my 
Apple Silicon machine.

We should make snapshot-boundary-restart deterministic: force enough unique writes, wait for and 
log per-node snapshot telemetry, then restart a compacted follower. Once that is in place, an 
Apple-only failure would be much more likely to point at a real timing, persistence, or snapshot 
recovery bug in the Raft implementation rather than in the Jepsen scenario setup.
