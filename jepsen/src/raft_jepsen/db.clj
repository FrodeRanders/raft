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
  (or (first (keep-indexed (fn [idx candidate]
                             (when (= candidate node) idx))
                           (:nodes test)))
      (throw (ex-info "Unknown Jepsen node" {:node node :nodes (:nodes test)}))))

(defn node-port [test node]
  (+ (long (:base-port test)) (long (node-index test node))))

(defn peer-spec [test node]
  (str node "@127.0.0.1:" (node-port test node)))

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
  (when-let [^Process process (get @processes node)]
    (.destroy process)
    (when-not (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 java.util.concurrent.TimeUnit/SECONDS))
    (swap! processes dissoc node)))

(defn wipe-node! [test node]
  (stop-node! test node)
  (delete-recursively! (node-root test node)))

(defn- await-port! [port timeout-ms]
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

(defn resolved-jar-path [test]
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

(defn start-node!
  ([test node]
   (start-node! test node false))
  ([test node preserve-state?]
   (when-not (process-alive? (get @processes node))
     (let [root (node-root test node)
           data-dir (node-data-dir test node)
           log-file (node-log-file test node)
           self-spec (peer-spec test node)
           peer-specs (->> (:nodes test)
                           (remove #(= % node))
                           (map #(peer-spec test %)))
           command (into ["java"
                          (str "-Draft.data.dir=" (.getAbsolutePath data-dir))
                          "-jar"
                          (resolved-jar-path test)
                          self-spec]
                         peer-specs)
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
                         {:node node :port (node-port test node) :log-file (.getAbsolutePath log-file)})))))))

(defn local-db []
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
