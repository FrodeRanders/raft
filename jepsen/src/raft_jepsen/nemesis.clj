(ns raft-jepsen.nemesis
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [raft-jepsen.db :as raft-db]
            [raft-jepsen.observer :as observer]))

(defn- random-node
  ([nodes]
   (when (seq nodes)
     (rand-nth (vec nodes))))
  ([test nodes]
   (random-node nodes)))

(defn crash-restart []
  (let [disrupted-node (atom nil)]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (locking disrupted-node
          (case (:f op)
            :start (if-let [node @disrupted-node]
                       (do
                         (raft-db/start-node! test node true)
                         (observer/capture-safe! test "nemesis-crash-restart" {:node node :op {:f :start}})
                         (reset! disrupted-node nil)
                         (assoc op :type :info :value {node :restarted}))
                       (assoc op :type :info :value :not-started))
            :stop (if @disrupted-node
                    (assoc op :type :info :value :already-stopped)
                    (if-let [node (random-node test (:nodes test))]
                    (do
                      (raft-db/stop-node! test node)
                      (observer/capture-safe! test "nemesis-crash-restart" {:node node :op {:f :stop}})
                      (reset! disrupted-node node)
                      (assoc op :type :info :value {node :stopped}))
                      (assoc op :type :info :value :no-target)))
            (assoc op :type :info :error :unsupported-nemesis-op))))
      (teardown! [_ test]
        (when-let [node @disrupted-node]
          (raft-db/start-node! test node true)
          (reset! disrupted-node nil))))))

(defn- partition-script [test]
  (.getAbsolutePath (io/file (:repo-root test) "jepsen" "scripts" "partition.sh")))

(defn- run-script! [test & args]
  (let [{:keys [exit err out]}
        (apply sh/sh (concat [(partition-script test)] args [:dir (:repo-root test)]))]
    (when-not (zero? exit)
      (throw (ex-info "Partition helper failed" {:args args :exit exit :out out :err err})))
    out))

(defn- java-command [jar-path & args]
  (vec (concat ["java" "-jar" jar-path] args)))

(defn- shell! [repo-root command]
  (apply sh/sh (concat command [:dir repo-root])))

(defn- parse-json [text]
  (when-let [trimmed (some-> text str/trim not-empty)]
    (try
      (json/parse-string trimmed true)
      (catch Throwable _
        nil))))

(defn- target-spec [test node]
  (str node "@127.0.0.1:" (raft-db/node-port test node)))

(defn- cluster-summary [test node]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test) "cluster-summary" "--json" (target-spec test node)))
        response (parse-json out)]
    (when-not (zero? exit)
      (throw (ex-info "Cluster summary command failed"
                      {:node node :exit exit :out out :err err})))
    response))

(defn- leader-node [test]
  (some (fn [node]
          (when-let [response (cluster-summary test node)]
            (let [leader-id (some-> (:leaderId response) str/trim not-empty)]
              (when (and (:success response)
                         leader-id
                         (some #(= leader-id %) (:nodes test)))
                leader-id))))
        (:nodes test)))

(defn- target-nodes [target]
  (cond
    (nil? target) []
    (sequential? target) (vec (remove nil? target))
    :else [target]))

(defn- isolate-nodes! [test nodes]
  (let [ports (mapv #(raft-db/node-port test %) nodes)]
    (apply run-script! test "isolate" (map str ports))
    {:nodes (vec nodes)
     :ports ports}))

(defn- heal-isolation! [test isolation]
  (when isolation
    (apply run-script! test "heal" (map str (:ports isolation)))))

(defn- leader-minority-nodes [test]
  (when-let [leader (leader-node test)]
    (let [followers (remove #(= leader %) (:nodes test))]
      (when-let [other (random-node followers)]
        [leader other]))))

(defn- partition-nemesis [target-node-fn context-name]
  (let [isolated (atom nil)]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start (if-let [nodes (seq (target-nodes (target-node-fn test)))]
                   (let [isolation (isolate-nodes! test nodes)
                         focus-node (first (:nodes isolation))]
                     (reset! isolated isolation)
                     (observer/capture-safe! test context-name {:node focus-node :op {:f :start}
                                                                :extra {:isolatedNodes (:nodes isolation)
                                                                        :isolatedPorts (:ports isolation)}})
                     (assoc op :type :info :value isolation))
                   (assoc op :type :info :value :no-target))
          :stop (do
                  (when @isolated
                    (heal-isolation! test @isolated)
                    (observer/capture-safe! test context-name {:node (first (:nodes @isolated)) :op {:f :stop}
                                                               :extra {:isolatedNodes (:nodes @isolated)
                                                                       :isolatedPorts (:ports @isolated)}})
                    (reset! isolated nil))
                  (assoc op :type :info))
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ test]
        (when @isolated
          (heal-isolation! test @isolated)
          (reset! isolated nil))))))

(defn partition-one []
  (partition-nemesis #(random-node (:nodes %)) "nemesis-partition"))

(defn partition-leader []
  (partition-nemesis leader-node "nemesis-partition-leader"))

(defn partition-leader-minority []
  (partition-nemesis leader-minority-nodes "nemesis-partition-leader-minority"))
