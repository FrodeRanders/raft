(ns raft-jepsen.observer
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]))

(defn- observations-dir [test]
  (doto (io/file (:workdir test) "observations")
    (.mkdirs)))

(defn- observations-file [test]
  (io/file (observations-dir test) "cluster-events.jsonl"))

(defn- append-json-line! [test event]
  (spit (observations-file test)
        (str (json/generate-string event) "\n")
        :append true))

(defn- parse-json [text]
  (when-let [trimmed (some-> text str/trim not-empty)]
    (try
      (json/parse-string trimmed true)
      (catch Throwable _
        nil))))

(defn- java-command [jar-path & args]
  (vec (concat ["java" "-jar" jar-path] args)))

(defn- shell! [repo-root command]
  (apply sh/sh (concat command [:dir repo-root])))

(defn- capture-command [test node command-name command]
  (let [{:keys [exit out err]} (shell! (:repo-root test) command)]
    {:node node
     :command command-name
     :exit exit
     :stdout (when-not (str/blank? out) out)
     :stderr (when-not (str/blank? err) err)
     :response (parse-json out)}))

(defn- node-index [test node]
  (or (first (keep-indexed (fn [idx candidate]
                             (when (= candidate node) idx))
                           (:nodes test)))
      (throw (ex-info "Unknown Jepsen node" {:node node :nodes (:nodes test)}))))

(defn- node-port [test node]
  (+ (long (:base-port test)) (long (node-index test node))))

(defn- target-spec [test node]
  (str node "@127.0.0.1:" (node-port test node)))

(defn- resolved-jar-path [test]
  (if-let [configured (:jar-path test)]
    (.getAbsolutePath (io/file configured))
    (let [target-dir (io/file (:repo-root test) "raft-dist" "target")
          jars (->> (or (.listFiles target-dir) [])
                    (filter #(and (.isFile ^java.io.File %)
                                  (.startsWith (.getName ^java.io.File %) "raft-")
                                  (.endsWith (.getName ^java.io.File %) ".jar")))
                    (sort-by #(.lastModified ^java.io.File %))
                    reverse)]
      (if-let [jar (first jars)]
        (.getAbsolutePath ^java.io.File jar)
        (throw (ex-info "No raft-dist jar found; build raft-dist first"
                        {:target-dir (.getAbsolutePath target-dir)}))))))

(defn capture! [test context & [{:keys [node op extra]
                                 :or {extra {}}}]]
  (let [jar-path (resolved-jar-path test)
        nodes (if node [node] (:nodes test))
        base-event {:observedAtMillis (System/currentTimeMillis)
                    :context context
                    :focusNode node
                    :op op}
        events (for [current-node nodes]
                 (merge
                  base-event
                  extra
                  {:observedNode current-node
                   :captures [(capture-command
                               test
                               current-node
                               "telemetry-summary"
                               (java-command jar-path "telemetry" "--json" "--summary" (target-spec test current-node)))
                              (capture-command
                               test
                               current-node
                               "cluster-summary"
                               (java-command jar-path "cluster-summary" "--json" (target-spec test current-node)))]}))]
    (doseq [event events]
      (append-json-line! test event))))

(defn capture-safe! [test context & [opts]]
  (try
    (capture! test context opts)
    (catch Throwable t
      (append-json-line!
       test
       {:observedAtMillis (System/currentTimeMillis)
        :context context
        :observerError (.getMessage t)
        :focusNode (:node opts)
        :op (:op opts)}))))
