(ns raft-jepsen.db
  (:require [clojure.java.io :as io]
            [jepsen.db :as db]
            [raft-jepsen.observer :as observer])
  (:import (java.io File)
           (java.lang Process ProcessBuilder ProcessBuilder$Redirect Thread)
           (java.net InetSocketAddress Socket)
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)))

(defonce processes (atom {}))

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

(defn stop-node! [test node]
  ;; DB and nemesis code both use this helper. A Jepsen DB should leave the
  ;; system in a known state after teardown, so process termination is explicit
  ;; and falls back to destroyForcibly if graceful shutdown hangs.
  (when-let [^Process process (get @processes node)]
    (.destroy process)
    (when-not (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS))
    (swap! processes dissoc node)))

(defn wipe-node! [test node]
  ;; Normal DB teardown removes the whole node work directory. That gives each
  ;; Jepsen test run a fresh cluster unless a nemesis intentionally preserves
  ;; or deletes selected state.
  (stop-node! test node)
  (delete-recursively! (node-root test node)))

(defn wipe-node-data! [test node]
  ;; Persistence-loss tests deliberately delete only the durable Raft state for
  ;; one stopped node, then restart it to check catch-up and snapshot recovery.
  (stop-node! test node)
  (delete-recursively! (node-data-dir test node)))

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
        process (.start builder)]
    (swap! processes assoc node process)
    (when-not (await-port! (node-port test node) 15000)
      (stop-node! test node)
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

(defn start-node!
  ([test node]
   (start-node! test node false))
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
