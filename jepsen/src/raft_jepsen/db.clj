(ns raft-jepsen.db
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.db :as db]
            [raft-jepsen.observer :as observer])
  (:import (java.io File)
           (java.lang Process ProcessBuilder ProcessBuilder$Redirect Thread)
           (java.net InetSocketAddress Socket)
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)))

(defonce processes (atom {}))
(defonce docker-state (atom {:started? false
                             :teardown-nodes #{}
                             :isolated-nodes #{}}))
(defonce clock-offsets (atom {}))

(declare await-port! resolved-jar-path start-node!)

(defn node-index [test node]
  ;; Jepsen node names are strings like "n1". The local harness maps them to
  ;; deterministic localhost ports so clients, nemeses, and observers can all
  ;; derive the same address from the test map.
  (or (first (keep-indexed (fn [idx candidate]
                             (when (= candidate node) idx))
                           (:nodes test)))
      (when-let [[_ ordinal] (re-matches #"n(\d+)" (str node))]
        (dec (Long/parseLong ordinal)))
      (throw (ex-info "Unknown Jepsen node" {:node node :nodes (:nodes test)}))))

(defn node-port [test node]
  (+ (long (:base-port test)) (long (node-index test node))))

(defn peer-spec [test node]
  (str node "@127.0.0.1:" (node-port test node)))

(defn membership-node [test]
  (str "n" (inc (long (:node-count test)))))

(defn membership-seed-node [test]
  (first (:nodes test)))

(defn node-ports [test]
  (mapv #(node-port test %) (:nodes test)))

(defn repo-root [test]
  (io/file (:repo-root test)))

(defn work-root [test]
  (io/file (:workdir test)))

(defn node-root [test node]
  (io/file (work-root test) node))

(defn node-data-dir [test node]
  (io/file (node-root test node) "data"))

(defn node-log-file [test node]
  (io/file (node-root test node) "raft.log"))

(defn node-impl [test node]
  (get (:node-impls test) node :java))

(defn docker-backend? [test]
  (= :docker-srv (:backend test)))

(defn joining-node-impl [test]
  (get test :joining-impl :java))

(defn- cpp-state-file [test node]
  (io/file (node-root test node) "graft.state"))

(defn- ensure-dir! [^File dir]
  (.mkdirs dir)
  dir)

(defn- delete-recursively! [^File file]
  (when (.exists file)
    (when (.isDirectory file)
      (doseq [child (or (.listFiles file) [])]
        (delete-recursively! child)))
    (Files/deleteIfExists (.toPath file))))

(defn- process-alive? [^Process process]
  (and process (.isAlive process)))

(defn- local-stop-node! [test node]
  ;; DB and nemesis code both use this helper. A Jepsen DB should leave the
  ;; system in a known state after teardown, so process termination is explicit
  ;; and falls back to destroyForcibly if graceful shutdown hangs.
  (when-let [^Process process (get @processes node)]
    (.destroy process)
    (when-not (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS))
    (swap! processes dissoc node)))

(defn- docker-compose-file [test]
  (io/file (repo-root test) "docker" (str "compose-srv-" (name (:srv-mode test)) ".yml")))

(defn- docker-project [test]
  (or (:compose-project test) "raft-jepsen-srv"))

(defn- docker-network [test]
  (str (docker-project test) "_raft-net"))

(defn- docker-node-ip [test node]
  (str "10.77.0." (+ 11 (node-index test node))))

(defn- docker-compose! [test & args]
  (let [command (concat ["docker" "compose"
                         "-p" (docker-project test)
                         "-f" (.getAbsolutePath (docker-compose-file test))]
                        args)
        {:keys [exit out err]} (apply sh/sh (concat command [:dir (.getAbsolutePath (repo-root test))]))]
    (when-not (zero? exit)
      (throw (ex-info "Docker Compose command failed"
                      {:command command :exit exit :out out :err err})))
    out))

(defn- docker! [test & args]
  (let [command (concat ["docker"] args)
        {:keys [exit out err]} (apply sh/sh (concat command [:dir (.getAbsolutePath (repo-root test))]))]
    (when-not (zero? exit)
      (throw (ex-info "Docker command failed"
                      {:command command :exit exit :out out :err err})))
    out))

(defn- docker-node-log-file [test node]
  (io/file (node-root test node) "docker.log"))

(defn- parse-json [text]
  (when-let [trimmed (some-> text str/trim not-empty)]
    (try
      (json/parse-string trimmed true)
      (catch Throwable _
        nil))))

(defn- cluster-summary [test node]
  (let [target (peer-spec test node)
        command ["java" "-jar" (resolved-jar-path test) "cluster-summary" "--json" target]
        {:keys [exit out]} (apply sh/sh (concat command [:dir (.getAbsolutePath (repo-root test))]))]
    (when (zero? exit)
      (parse-json out))))

(defn- await-healthy-leader! [test timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [healthy? (some (fn [node]
                             (try
                               (let [summary (cluster-summary test node)]
                                 (and (:success summary)
                                      (= "healthy" (:clusterHealth summary))))
                               (catch Throwable _
                                 false)))
                           (:nodes test))]
        (cond
          healthy? true
          (< (System/currentTimeMillis) deadline) (do
                                                    (Thread/sleep 500)
                                                    (recur))
          :else (throw (ex-info "Timed out waiting for healthy Docker/SRV leader"
                                {:nodes (:nodes test)
                                 :ports (node-ports test)})))))))

(defn- capture-docker-logs! [test node]
  (ensure-dir! (node-root test node))
  (let [{:keys [out err]}
        (apply sh/sh (concat ["docker" "compose"
                              "-p" (docker-project test)
                              "-f" (.getAbsolutePath (docker-compose-file test))
                              "logs" "--no-color" (str node)]
                             [:dir (.getAbsolutePath (repo-root test))]))]
    (spit (docker-node-log-file test node) (str out err))))

(defn- ensure-docker-cluster! [test]
  ;; Docker/SRV mode starts the whole Compose topology once. Individual Jepsen
  ;; setup! calls still wait for their node's published port, but the lifecycle
  ;; boundary is the Compose project: CoreDNS plus all Raft services.
  (locking docker-state
    (when-not (:started? @docker-state)
      (ensure-dir! (work-root test))
      (docker-compose! test "down" "-v" "--remove-orphans")
      (docker-compose! test "up" "-d" "--build")
      (doseq [node (:nodes test)]
        (when-not (await-port! (node-port test node) 30000)
          (throw (ex-info "Timed out waiting for Docker/SRV node"
                          {:node node :port (node-port test node)}))))
      (await-healthy-leader! test 60000)
      (reset! docker-state {:started? true
                            :teardown-nodes #{}
                            :isolated-nodes #{}}))))

(defn stop-node! [test node]
  (if (docker-backend? test)
    (docker-compose! test "stop" (str node))
    (local-stop-node! test node)))

(defn- signal-local-node! [node signal]
  (if-let [^Process process (get @processes node)]
    (let [pid (.pid process)
          {:keys [exit out err]} (sh/sh "kill" (str signal) (str pid))]
      (when-not (zero? exit)
        (throw (ex-info "Failed to signal local node process"
                        {:node node :pid pid :signal signal :exit exit :out out :err err}))))
    (throw (ex-info "No local process for node"
                    {:node node}))))

(defn pause-node! [test node]
  (if (docker-backend? test)
    (try
      (docker-compose! test "pause" (str node))
      (catch Throwable t
        (if (str/includes? (or (:err (ex-data t)) "") "is already paused")
          nil
          (throw t))))
    (signal-local-node! node "-STOP")))

(defn resume-node! [test node]
  (if (docker-backend? test)
    (try
      (docker-compose! test "unpause" (str node))
      (catch Throwable t
        (if (str/includes? (or (:err (ex-data t)) "") "is not paused")
          nil
          (throw t))))
    (signal-local-node! node "-CONT")))

(defn set-node-clock-offset! [test node offset-millis]
  ;; Clock skew is injected through the Raft logical clock, not the host clock.
  ;; Restarting the process is enough because both Java and C++ startup paths
  ;; read RAFT_CLOCK_OFFSET_MILLIS into their RaftNode time source.
  (when (docker-backend? test)
    (throw (ex-info "Docker/SRV clock skew needs generated service environment updates"
                    {:node node :backend (:backend test)})))
  (stop-node! test node)
  (swap! clock-offsets assoc node (long offset-millis))
  (start-node! test node true))

(defn clear-node-clock-offset! [test node]
  (when (docker-backend? test)
    (throw (ex-info "Docker/SRV clock skew needs generated service environment updates"
                    {:node node :backend (:backend test)})))
  (stop-node! test node)
  (swap! clock-offsets dissoc node)
  (start-node! test node true))

(defn wipe-node! [test node]
  ;; Normal DB teardown removes the whole node work directory. That gives each
  ;; Jepsen test run a fresh cluster unless a nemesis intentionally preserves
  ;; or deletes selected state.
  (local-stop-node! test node)
  (swap! clock-offsets dissoc node)
  (delete-recursively! (node-root test node)))

(defn wipe-node-data! [test node]
  ;; Persistence-loss tests deliberately delete only the durable Raft state for
  ;; one stopped node, then restart it to check catch-up and snapshot recovery.
  (if (docker-backend? test)
    (throw (ex-info "Docker/SRV persistence-loss restart is not implemented; use crash-restart or partition scenarios"
                    {:node node :backend (:backend test)}))
    (do
      (local-stop-node! test node)
      (delete-recursively! (node-data-dir test node)))))

(defn- await-port! [port timeout-ms]
  ;; setup! should not return until the node can accept client connections.
  ;; Without this readiness gate, early Jepsen operations would mostly test
  ;; startup races instead of Raft behavior.
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [connected?
            (try
              (with-open [socket (Socket.)]
                (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 250)
                true)
              (catch Exception _
                false))]
        (cond
          connected? true
          (< (System/currentTimeMillis) deadline) (do
                                                    (Thread/sleep 100)
                                                    (recur))
          :else false)))))

(defn- launch-node! [test node command preserve-state?]
  ;; Shared local process lifecycle for all node implementations. The caller
  ;; is responsible for building the implementation-specific command line; this
  ;; function only handles work directories, stdout/stderr capture, and port
  ;; readiness. That makes it the right boundary for adding C++ nodes.
  (let [root (node-root test node)
        data-dir (node-data-dir test node)
        log-file (node-log-file test node)
        _ (when-not preserve-state?
            (delete-recursively! root))
        _ (ensure-dir! data-dir)
        builder (doto (ProcessBuilder. ^java.util.List command)
                  (.directory (repo-root test))
                  (.redirectErrorStream true)
                  (.redirectOutput (ProcessBuilder$Redirect/appendTo log-file)))
        env (.environment builder)
        offset (long (get @clock-offsets node 0))
        _ (if (zero? offset)
            (.remove env "RAFT_CLOCK_OFFSET_MILLIS")
            (.put env "RAFT_CLOCK_OFFSET_MILLIS" (str offset)))
        process (.start builder)]
    (swap! processes assoc node process)
    (when-not (await-port! (node-port test node) 15000)
      (local-stop-node! test node)
      (throw (ex-info "Timed out waiting for node to start"
                      {:node node :port (node-port test node) :log-file (.getAbsolutePath log-file)})))))

(defn resolved-jar-path [test]
  ;; Most Jepsen harnesses copy binaries to remote machines. This local harness
  ;; resolves artifacts from the repository instead, while still allowing an
  ;; explicit --jar override for reproducible runs.
  (if-let [configured (:jar-path test)]
    (.getAbsolutePath (io/file configured))
    (let [target-dir (io/file (repo-root test) "raft-dist" "target")
          jars (->> (or (.listFiles target-dir) [])
                    (filter #(and (.isFile ^File %)
                                  (.startsWith (.getName ^File %) "raft-")
                                  (.endsWith (.getName ^File %) ".jar")))
                    (sort-by #(.lastModified ^File %))
                    reverse)]
      (if-let [jar (first jars)]
        (.getAbsolutePath ^File jar)
        (throw (ex-info "No raft-dist jar found; build raft-dist first"
                        {:target-dir (.getAbsolutePath target-dir)}))))))

(defn resolved-cpp-bin [test]
  ;; C++ support is optional. Resolve the smoke binary lazily so Java-only runs
  ;; do not depend on the C++ build.
  (if-let [configured (:cpp-bin test)]
    (.getAbsolutePath (io/file configured))
    (let [candidates [(io/file (repo-root test) "graft-cpp" "build" "graft_smoke")
                      (io/file "/tmp" "graft-cpp-build" "graft_smoke")]
          found (first (filter #(.canExecute ^File %) candidates))]
      (if found
        (.getAbsolutePath ^File found)
        (throw (ex-info "No executable graft_smoke found; build graft-cpp or pass --cpp-bin"
                        {:candidates (mapv #(.getAbsolutePath ^File %) candidates)}))))))

(defn- java-props [test node]
  ;; Per-node JVM properties are part of the DB command line because Jepsen
  ;; treats each node as a separate process with its own data directory.
  (vec
   (concat
    [(str "-Draft.data.dir=" (.getAbsolutePath (node-data-dir test node)))]
    (when-let [min-entries (:snapshot-min-entries test)]
      [(str "-Draft.snapshot.min.entries=" (long min-entries))])
    (when-let [chunk-bytes (:snapshot-chunk-bytes test)]
      [(str "-Draft.snapshot.chunk.bytes=" (long chunk-bytes))]))))

(defn- start-local-node!
  ([test node]
   (start-local-node! test node false))
  ([test node preserve-state?]
   ;; Mixed clusters dispatch here. Java and C++ nodes share stable peer ids,
   ;; host-local ports, and the generic process lifecycle above; only the
   ;; command line and persistence path differ by implementation.
   (when-not (process-alive? (get @processes node))
     (let [peer-specs (->> (:nodes test)
                           (remove #(= % node))
                           (map #(peer-spec test %)))
           command (case (node-impl test node)
                     :java (vec (concat ["java"]
                                        (java-props test node)
                                        ["-jar"
                                         (resolved-jar-path test)
                                         (peer-spec test node)]
                                        peer-specs))
                     :cpp (vec (concat [(resolved-cpp-bin test)
                                        "serve-active-persistent"
                                        "127.0.0.1"
                                        (str (node-port test node))
                                        (str node)
                                        (.getAbsolutePath (cpp-state-file test node))
                                        "0"
                                        "0"
                                        "0"]
                                       peer-specs)))]
       (launch-node! test node command preserve-state?)))))

(defn- start-docker-node! [test node]
  ;; Restarting a Docker/SRV node preserves its named volume, matching the local
  ;; crash-restart semantics where durable Raft state survives process death.
  (docker-compose! test "start" (str node))
  (when-not (await-port! (node-port test node) 30000)
    (throw (ex-info "Timed out waiting for Docker node to start"
                    {:node node :port (node-port test node)}))))

(defn start-node!
  ([test node]
   (start-node! test node false))
  ([test node preserve-state?]
   (if (docker-backend? test)
     (start-docker-node! test node)
     (start-local-node! test node preserve-state?))))

(defn start-joining-node!
  ([test node]
   (start-joining-node! test node false))
  ([test node preserve-state?]
   ;; Membership nemeses introduce one extra learner node outside the initial
   ;; :nodes set. Java joiners use raft-dist join mode; C++ joiners mirror the
   ;; mixed smoke tests by starting as passive persistent servers and waiting
   ;; for the leader's explicit join request.
   (when-not (process-alive? (get @processes node))
     (let [seed (membership-seed-node test)
           peer-specs (mapv #(peer-spec test %) (:nodes test))
           command (case (joining-node-impl test)
                     :java (vec (concat ["java"]
                                        (java-props test node)
                                        ["-jar"
                                         (resolved-jar-path test)
                                         "join"
                                         (str (peer-spec test node) "/learner")
                                         (peer-spec test seed)]))
                     :cpp (vec (concat [(resolved-cpp-bin test)
                                        "serve-persistent"
                                        "127.0.0.1"
                                        (str (node-port test node))
                                        (str node)
                                        (.getAbsolutePath (cpp-state-file test node))
                                        "0"
                                        "0"
                                        "0"]
                                       peer-specs)))]
       (launch-node! test node command preserve-state?)))))

(defn local-db []
  ;; DB is Jepsen's system lifecycle abstraction. setup!/teardown! run once per
  ;; node around the test. The Kill extension is what lets nemeses stop and
  ;; restart nodes using Jepsen's standard db/kill! and db/start! concepts.
  (reify db/DB
    (setup! [_ test node]
      (start-node! test node false)
      (observer/capture-safe! test "db-setup" {:node node}))
    (teardown! [_ test node]
      (observer/capture-safe! test "db-teardown" {:node node})
      (wipe-node! test node))
    db/Kill
    (kill! [_ test node]
      (stop-node! test node))
    (start! [_ test node]
      (start-node! test node true))
    db/LogFiles
    (log-files [_ test node]
      [(.getAbsolutePath (node-log-file test node))])))

(defn docker-isolate-nodes! [test nodes]
  ;; A Docker/SRV partition is modeled by disconnecting containers from the
  ;; Compose network. This blocks peer traffic and host client probes for those
  ;; nodes while leaving their process and volume intact.
  (let [nodes (vec nodes)]
    (doseq [node nodes]
      (try
        (docker! test "network" "disconnect" (docker-network test) (str node))
        (catch Throwable _
          nil)))
    (swap! docker-state update :isolated-nodes into nodes)
    {:nodes nodes
     :network (docker-network test)}))

(defn docker-heal-isolation! [test isolation]
  (doseq [node (:nodes isolation)]
    (try
      (docker! test "network" "connect" "--ip" (docker-node-ip test node) (docker-network test) (str node))
      (catch Throwable _
        nil)))
  (swap! docker-state update :isolated-nodes #(apply disj % (:nodes isolation))))

(defn docker-db []
  (reify db/DB
    (setup! [_ test node]
      (ensure-docker-cluster! test)
      (when-not (await-port! (node-port test node) 30000)
        (throw (ex-info "Timed out waiting for Docker/SRV node"
                        {:node node :port (node-port test node)})))
      (observer/capture-safe! test "docker-db-setup" {:node node}))
    (teardown! [_ test node]
      (observer/capture-safe! test "docker-db-teardown" {:node node})
      (capture-docker-logs! test node)
      (locking docker-state
        (swap! docker-state update :teardown-nodes conj node)
        (when (and (:started? @docker-state)
                   (= (set (:nodes test)) (:teardown-nodes @docker-state)))
          (docker-compose! test "down" "-v" "--remove-orphans")
          (reset! docker-state {:started? false
                                :teardown-nodes #{}
                                :isolated-nodes #{}}))))
    db/Kill
    (kill! [_ test node]
      (stop-node! test node))
    (start! [_ test node]
      (start-node! test node true))
    db/LogFiles
    (log-files [_ test node]
      [(.getAbsolutePath (docker-node-log-file test node))])))
