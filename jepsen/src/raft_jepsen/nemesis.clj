(ns raft-jepsen.nemesis
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
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

(defn partition-one []
  (let [isolated (atom nil)]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start (let [node (random-node (:nodes test))
                       port (raft-db/node-port test node)]
                   (run-script! test "isolate" (str port))
                   (reset! isolated {:node node :port port})
                   (observer/capture-safe! test "nemesis-partition" {:node node :op {:f :start}
                                                                     :extra {:isolatedPort port}})
                   (assoc op :type :info :value @isolated))
          :stop (do
                  (when @isolated
                    (run-script! test "heal" (str (:port @isolated)))
                    (observer/capture-safe! test "nemesis-partition" {:node (:node @isolated) :op {:f :stop}
                                                                      :extra {:isolatedPort (:port @isolated)}})
                    (reset! isolated nil))
                  (assoc op :type :info))
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ test]
        (when @isolated
          (run-script! test "heal" (str (:port @isolated)))
          (reset! isolated nil))))))
