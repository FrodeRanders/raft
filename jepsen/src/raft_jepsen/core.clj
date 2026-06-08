(ns raft-jepsen.core
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.core :as jepsen]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [jepsen.checker.timeline :as timeline]
            [raft-jepsen.client :as raft-client]
            [raft-jepsen.db :as raft-db]
            [raft-jepsen.local-remote :as local-remote]
            [raft-jepsen.nemesis :as raft-nemesis]
            [knossos.model :as model]))

(defn- parse-long-arg [value]
  (Long/parseLong (str value)))

(defn- parse-bool-arg [value]
  (case (str/lower-case (str value))
    "true" true
    "false" false
    (throw (ex-info "Expected boolean value" {:value value}))))

(defn- node-names [node-count]
  (mapv #(str "n" %) (range 1 (inc node-count))))

(defn- parse-node-impls [value]
  (mapv #(keyword (str/lower-case (str/trim %)))
        (str/split (str value) #",")))

(defn- default-repo-root []
  (.getCanonicalPath (io/file "..")))

(defn- default-workdir []
  (.getCanonicalPath (io/file "work")))

(defn- docker-node-names [node-count]
  (mapv #(str "raft-" %) (range 1 (inc node-count))))

(defn- parse-args [args]
  ;; Jepsen tests are ordinary Clojure values, so this harness keeps CLI
  ;; parsing deliberately small: parse flags into one opts map, validate it in
  ;; normalize-opts, then pass the same opts through test construction.
  (loop [opts {:repo-root (default-repo-root)
               :workdir (default-workdir)
               ;; Mixed Java/C++ support is explicit per node so peer ids and
               ;; port assignments stay independent from implementation type.
               :node-impl-list nil
               :client-impl :java
               :joining-impl :java
               :cpp-bin nil
               :base-port 10080
               :time-limit 30
               :concurrency 10
               :nemesis-mode "none"
               :nemesis-interval 5
               :clock-skew-millis 5000
               :workload-mode "single-key"
               :key-count 3
               :operation-limit 100
               :unique-values false
               :snapshot-min-entries nil
               :snapshot-chunk-bytes nil
               :node-count 5
               :nodes (node-names 5)
               :key "k"}
         remaining args]
    (if (empty? remaining)
      opts
      (let [[flag value & rest] remaining]
        (case flag
          "--jar" (recur (assoc opts :jar-path (.getCanonicalPath (io/file value))) rest)
          "--cpp-bin" (recur (assoc opts :cpp-bin (.getCanonicalPath (io/file value))) rest)
          "--node-impls" (recur (assoc opts :node-impl-list (parse-node-impls value)) rest)
          "--client-impl" (recur (assoc opts :client-impl (keyword (str/lower-case value))) rest)
          "--joining-impl" (recur (assoc opts :joining-impl (keyword (str/lower-case value))) rest)
          "--backend" (recur (assoc opts :backend (keyword (str/lower-case value))) rest)
          "--srv-mode" (recur (assoc opts :srv-mode (keyword (str/lower-case value))) rest)
          "--compose-project" (recur (assoc opts :compose-project value) rest)
          "--time-limit" (recur (assoc opts :time-limit (parse-long-arg value)) rest)
          "--concurrency" (recur (assoc opts :concurrency (parse-long-arg value)) rest)
          "--base-port" (recur (assoc opts :base-port (parse-long-arg value)) rest)
          "--workload" (recur (assoc opts :workload-mode value) rest)
          "--key-count" (recur (assoc opts :key-count (parse-long-arg value)) rest)
          "--operation-limit" (recur (assoc opts :operation-limit (parse-long-arg value)) rest)
          "--unique-values" (recur (assoc opts :unique-values (parse-bool-arg value)) rest)
          "--snapshot-min-entries" (recur (assoc opts :snapshot-min-entries (parse-long-arg value)) rest)
          "--snapshot-chunk-bytes" (recur (assoc opts :snapshot-chunk-bytes (parse-long-arg value)) rest)
          "--node-count" (let [count (parse-long-arg value)]
                           (recur (assoc opts
                                         :node-count count
                                         :nodes (node-names count))
                                  rest))
          "--nemesis" (recur (assoc opts :nemesis-mode value) rest)
          "--nemesis-interval" (recur (assoc opts :nemesis-interval (parse-long-arg value)) rest)
          "--clock-skew-millis" (recur (assoc opts :clock-skew-millis (parse-long-arg value)) rest)
          "--workdir" (recur (assoc opts :workdir (.getCanonicalPath (io/file value))) rest)
          "--help" (assoc opts :help true)
          (throw (ex-info "Unknown option" {:flag flag :remaining remaining})))))))

(defn- usage []
  (str/join
   \newline
   ["Usage: clojure -M:run [options]"
    ""
    "Options:"
    "  --jar <path>          Path to raft-dist jar"
    "  --cpp-bin <path>      Path to graft_smoke for C++ nodes"
    "  --node-impls <list>   Comma-separated node implementations, e.g. java,cpp,java"
    "  --client-impl <impl>  Client CLI implementation, java|cpp|mixed"
    "  --joining-impl <impl> Implementation for membership-join-promote joining node, java|cpp"
    "  --backend <backend>   local|docker-srv, default local"
    "  --srv-mode <mode>     Docker/SRV Compose mode, java|cpp|mixed"
    "  --compose-project <p> Docker Compose project name for docker-srv"
    "  --time-limit <sec>    Workload duration, default 30"
    "  --concurrency <n>     Client concurrency, default 10"
    "  --base-port <port>    First node port, default 10080"
    "  --workload <mode>     single-key|multi-key, default single-key"
    "  --key-count <n>       Keys for multi-key workload, default 3"
    "  --operation-limit <n> Maximum generated client operations, default 100"
    "  --unique-values <bool> Generate unique write/CAS values, default false"
    "  --snapshot-min-entries <n>  Override raft.snapshot.min.entries"
    "  --snapshot-chunk-bytes <n>  Override raft.snapshot.chunk.bytes"
    "  --node-count <n>      Number of local nodes, default 5"
    "  --nemesis <mode>      none|crash-restart|process-pause|clock-skew|persistence-loss-restart|snapshot-boundary-restart|partition-one|partition-leader|partition-leader-minority|membership-join-promote|membership-demote|membership-remove-follower|membership-remove-leader|membership-remove-follower-partition-leader, default none"
    "  --nemesis-interval <sec> Nemesis interval, default 5"
    "  --clock-skew-millis <ms> Logical clock offset for clock-skew nemesis, default 5000"
    "  --workdir <path>      Local work directory, default ./work"]))

(defn- normalize-opts [opts]
  ;; Normalize command-line sugar into values the rest of the harness can use.
  ;; The important Jepsen convention here is that :nodes are opaque node names;
  ;; implementation type, ports, and paths live in the test map instead of
  ;; being encoded into those names.
  (let [backend (get opts :backend :local)
        srv-mode (get opts :srv-mode :java)
        opts (cond-> opts
               (= :docker-srv backend)
               (assoc :node-count 3
                      :nodes (docker-node-names 3)
                      :base-port 17001
                      :srv-mode srv-mode))
        nodes (:nodes opts)
        impls (or (:node-impl-list opts)
                  (case srv-mode
                    :java (vec (repeat (count nodes) :java))
                    :cpp (vec (repeat (count nodes) :cpp))
                    :mixed [:java :cpp :cpp]
                    (vec (repeat (count nodes) :java))))
        joining-impl (:joining-impl opts)
        client-impl (:client-impl opts)
        allowed #{:java :cpp}]
    (when-not (#{:local :docker-srv} backend)
      (throw (ex-info "Unsupported backend"
                      {:backend backend :allowed #{:local :docker-srv}})))
    (when-not (#{:java :cpp :mixed} srv-mode)
      (throw (ex-info "Unsupported Docker/SRV mode"
                      {:srv-mode srv-mode :allowed #{:java :cpp :mixed}})))
    (when (and (= :docker-srv backend)
               (not (#{3} (:node-count opts))))
      (throw (ex-info "Docker/SRV backend currently uses the fixed three-node Compose topology"
                      {:node-count (:node-count opts)})))
    (when (and (= :docker-srv backend)
               (#{"persistence-loss-restart"
                  "clock-skew"
                  "snapshot-boundary-restart"
                  "membership-join-promote"
                  "membership-demote"
                  "membership-remove-follower"
                  "membership-remove-leader"
                  "membership-remove-follower-partition-leader"} (:nemesis-mode opts)))
      (throw (ex-info "Docker/SRV backend supports baseline, crash-restart, process-pause, and partition nemeses; clock skew, snapshot boundary, and dynamic membership need generated service definitions"
                      {:nemesis-mode (:nemesis-mode opts)})))
    (when-not (= (count nodes) (count impls))
      (throw (ex-info "--node-impls count must match --node-count"
                      {:node-count (count nodes)
                       :node-impl-count (count impls)
                       :node-impls impls})))
    (doseq [impl impls]
      (when-not (allowed impl)
        (throw (ex-info "Unsupported node implementation"
                        {:impl impl :allowed allowed}))))
    (when-not (allowed joining-impl)
      (throw (ex-info "Unsupported joining node implementation"
                      {:impl joining-impl :allowed allowed})))
    (when-not (#{:java :cpp :mixed} client-impl)
      (throw (ex-info "Unsupported client implementation"
                      {:impl client-impl :allowed #{:java :cpp :mixed}})))
    (-> opts
        (cond-> (= "snapshot-boundary-restart" (:nemesis-mode opts))
          (update :snapshot-min-entries #(or % 5))
          (= "snapshot-boundary-restart" (:nemesis-mode opts))
          (update :snapshot-chunk-bytes #(or % 1024)))
        (assoc :node-impls (zipmap nodes impls))
        (assoc :backend backend
               :srv-mode srv-mode)
        (dissoc :node-impl-list))))

(def cas-values ["v0" "v1" "v2" "v3" "v4"])

(defn- next-unique-value [value-state process]
  (let [process-id (if (nil? process) "p" (str "p" process))
        value (str process-id "-op" (swap! (:counter value-state) inc))]
    (swap! (:seen value-state) conj value)
    value))

(defn- generator-process [args]
  (let [candidate (first (filter integer? args))]
    candidate))

(defn- random-value [opts value-state process]
  (if (:unique-values opts)
    (next-unique-value value-state process)
    (rand-nth cas-values)))

(defn- random-expected [opts value-state]
  (if (:unique-values opts)
    (let [seen @(:seen value-state)]
      (if (seq seen)
        (rand-nth (vec (cons nil seen)))
        nil))
    (rand-nth (vec (cons nil cas-values)))))

(defn- workload-keys [opts]
  (if (= "multi-key" (:workload-mode opts))
    (mapv #(str "k" %) (range (max 1 (:key-count opts))))
    [(:key opts)]))

(defn- random-key [opts]
  (rand-nth (workload-keys opts)))

(defn- op-with-key [opts op]
  (assoc op :key (random-key opts)))

(defn- write-op [opts value-state & args]
  ;; Generators emit operation maps. Jepsen will pass each map to the client
  ;; invoke! method and record the returned map in the history.
  (let [process (generator-process args)]
    (op-with-key opts
    {:type :invoke
     :f :write
     :value (random-value opts value-state process)})))

(defn- read-op [opts & _]
  (op-with-key opts
  {:type :invoke
   :f :read}))

(defn- cas-op [opts value-state & args]
  (let [process (generator-process args)
        expected (random-expected opts value-state)
        next-value (loop [candidate (random-value opts value-state process)]
                     (if (= candidate expected)
                       (recur (random-value opts value-state process))
                       candidate))]
    (op-with-key opts
    {:type :invoke
     :f :cas
     :value [expected next-value]})))

(defn- filter-history-checker [inner-checker pred]
  ;; Checkers see the whole history, including nemesis events. Linearizability
  ;; models only client operations, so this wrapper trims the history before
  ;; delegating to Knossos.
  (reify
    checker/Checker
    (check [_ test history opts]
      (checker/check inner-checker test (filterv pred history) opts))))

(defn- key-history [history key]
  (->> history
       (filterv #(and (not= :nemesis (:process %))
                      (= key (:key %))))))

(defn- key-checker [key]
  ;; Knossos' CAS register model understands reads, writes, and CAS operations.
  ;; For multi-key workloads, each key is an independent register and must be
  ;; checked with only the operations that touched that key.
  (filter-history-checker
   (checker/linearizable {:model (model/cas-register)})
   #(= key (:key %))))

(defn- independent-key-checker [keys]
  (reify
    checker/Checker
    (check [_ test history opts]
      (let [results (into {}
                          (for [key keys]
                            [key (checker/check (key-checker key) test history opts)]))
            valid? (every? true? (map :valid? (vals results)))]
        {:valid? valid?
         :results results}))))

(defn- nemesis-error-checker []
  ;; Linearizability ignores nemesis operations by design, but membership
  ;; nemeses validate cluster progress. Treat their errors as test failures.
  (reify
    checker/Checker
    (check [_ _ history _]
      (let [errors (filterv #(and (= :nemesis (:process %))
                                  (or (:error %)
                                      (:exception %)))
                            history)]
        {:valid? (empty? errors)
         :errors errors}))))

(defn- workload [opts]
  ;; A Jepsen workload usually contributes two things: a generator that creates
  ;; client operations, and a checker that judges the completed history. This
  ;; function returns those two pieces so raft-test can merge them into the
  ;; final test map.
  (let [keys (workload-keys opts)
        value-state {:counter (atom 0)
                     :seen (atom [])}]
    {:checker (checker/compose
               {:linearizable (if (= "multi-key" (:workload-mode opts))
                                (independent-key-checker keys)
                                (filter-history-checker
                                 (checker/linearizable {:model (model/cas-register)})
                                 #(not= :nemesis (:process %))))
                :nemesis-errors (nemesis-error-checker)
                :timeline (timeline/html)})
     :generator (->> (gen/mix [(partial write-op opts value-state)
                               (partial read-op opts)
                               (partial cas-op opts value-state)])
                     (gen/limit (:operation-limit opts)))}))

(defn- nemesis-object [opts]
  ;; The nemesis object is the imperative part: Jepsen calls its invoke! method
  ;; when the nemesis generator emits {:f :start} or {:f :stop}. Each mode below
  ;; returns a different object implementing jepsen.nemesis/Nemesis.
  (case (:nemesis-mode opts)
    "none" nemesis/noop
    "crash-restart" (raft-nemesis/crash-restart)
    "process-pause" (raft-nemesis/process-pause)
    "clock-skew" (raft-nemesis/clock-skew)
    "persistence-loss-restart" (raft-nemesis/persistence-loss-restart)
    "snapshot-boundary-restart" (raft-nemesis/snapshot-boundary-restart)
    "partition-one" (raft-nemesis/partition-one)
    "partition-leader" (raft-nemesis/partition-leader)
    "partition-leader-minority" (raft-nemesis/partition-leader-minority)
    "membership-join-promote" (raft-nemesis/membership-join-promote)
    "membership-demote" (raft-nemesis/membership-demote)
    "membership-remove-follower" (raft-nemesis/membership-remove-follower)
    "membership-remove-leader" (raft-nemesis/membership-remove-leader)
    "membership-remove-follower-partition-leader" (raft-nemesis/membership-remove-follower-partition-leader)
    (throw (ex-info "Unknown nemesis mode" {:nemesis-mode (:nemesis-mode opts)}))))

(defn- nemesis-generator [opts]
  ;; The nemesis generator decides when fault operations are injected. It is
  ;; separate from the nemesis object, which decides what those operations do.
  ;; One-shot membership scenarios emit a single :start; partition scenarios
  ;; alternate :stop/:start in a cycle so failures and healing are both tested.
  (case (:nemesis-mode opts)
    "none" nil
    "membership-join-promote"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start}))
    "membership-demote"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start}))
    "membership-remove-follower"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start}))
    "snapshot-boundary-restart"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start}))
    "membership-remove-leader"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start}))
    "membership-remove-follower-partition-leader"
    (gen/phases
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :start})
     (gen/sleep (:nemesis-interval opts))
     (gen/once {:f :stop}))
    (gen/cycle
     (gen/phases
      (gen/once {:f :stop})
      (gen/sleep (:nemesis-interval opts))
      (gen/once {:f :start})
     (gen/sleep (:nemesis-interval opts))))))

(defn- heal-before-final-read-generator [opts]
  ;; Cyclic fault nemeses use :stop to inject the fault and :start to heal it.
  ;; Ensure the final read phase observes a settled cluster instead of leaving a
  ;; node crashed, paused, or partitioned until teardown.
  (when (#{"crash-restart"
           "process-pause"
           "clock-skew"
           "persistence-loss-restart"
           "partition-one"
           "partition-leader"
           "partition-leader-minority"} (:nemesis-mode opts))
    (gen/nemesis (gen/once {:f :start}))))

(defn- generator [opts]
  ;; The full generator combines client load and optional nemesis activity:
  ;; - gen/clients assigns workload ops to client threads
  ;; - gen/stagger spaces invocations out slightly
  ;; - gen/nemesis interleaves fault operations on the special :nemesis process
  ;; - gen/time-limit bounds the run
  ;; The final read phase is useful because it gives the checker a settled
  ;; observation after the last fault or write.
  (let [client-gen (:generator (workload opts))
        base (->> client-gen
                  gen/clients
                  (gen/stagger 1/10))
        with-nemesis (if-let [n-gen (nemesis-generator opts)]
                       (->> base
                            (gen/nemesis n-gen))
                       base)]
    (apply gen/phases
           (remove nil?
                   [(gen/time-limit (:time-limit opts) with-nemesis)
                    (heal-before-final-read-generator opts)
                    (gen/log "Waiting for cluster to settle")
                    (gen/sleep 5)
                    (gen/clients
                     (gen/each-thread
                      (gen/once {:f :read :value nil})))]))))

(defn raft-test [opts]
  ;; This is the Jepsen test map. jepsen/run! treats it as the complete
  ;; description of a test: what nodes exist, how to control them, what client
  ;; to run, what faults to inject, what history to generate, and how to check
  ;; the result.
  (merge
   tests/noop-test
   (workload opts)
    {:name (str "raft-kv-" (name (:backend opts)) "-" (:node-count opts) "n")
    :os os/noop
    :remote local-remote/remote
    :nodes (:nodes opts)
    ;; local-db starts and kills local OS processes, dispatching each node to
    ;; the Java or C++ command builder according to :node-impls.
    :db (case (:backend opts)
          :local (raft-db/local-db)
          :docker-srv (raft-db/docker-db))
    ;; The Jepsen client can drive operations through the Java CLI, the C++
    ;; graft_smoke CLI, or by matching each target node's implementation.
    :client (raft-client/client opts)
    :nemesis (nemesis-object opts)
    :generator (generator opts)
    :base-port (:base-port opts)
    :node-count (:node-count opts)
    :time-limit (:time-limit opts)
    :concurrency (:concurrency opts)
    :operation-limit (:operation-limit opts)
    :unique-values (:unique-values opts)
    :snapshot-min-entries (:snapshot-min-entries opts)
    :snapshot-chunk-bytes (:snapshot-chunk-bytes opts)
    :node-impls (:node-impls opts)
    :client-impl (:client-impl opts)
    :joining-impl (:joining-impl opts)
    :clock-skew-millis (:clock-skew-millis opts)
    :backend (:backend opts)
    :srv-mode (:srv-mode opts)
    :compose-project (:compose-project opts)
    :cpp-bin (:cpp-bin opts)
    :repo-root (:repo-root opts)
    :workdir (:workdir opts)
    :jar-path (:jar-path opts)
    :key (:key opts)}))

(defn -main [& args]
  ;; jepsen/run! is the entry point: it sets up the DB on every node, opens
  ;; clients, runs the generator, tears everything down, saves logs/history, and
  ;; invokes the checker.
  (let [opts (normalize-opts (parse-args args))]
    (if (:help opts)
      (println (usage))
      (do
        (let [opts (assoc opts :jar-path (raft-db/resolved-jar-path opts))]
          (println "Running Raft Jepsen harness with options:" (pr-str (dissoc opts :help)))
          (try
            (let [result (jepsen/run! (raft-test opts))
                  valid? (or (:valid? result)
                             (get-in result [:results :valid?]))]
              (when-not valid?
                (System/exit 1)))
            (finally
              (shutdown-agents))))))))
