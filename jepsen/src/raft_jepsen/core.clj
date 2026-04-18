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

(defn- node-names [node-count]
  (mapv #(str "n" %) (range 1 (inc node-count))))

(defn- default-repo-root []
  (.getCanonicalPath (io/file "..")))

(defn- default-workdir []
  (.getCanonicalPath (io/file "work")))

(defn- parse-args [args]
  (loop [opts {:repo-root (default-repo-root)
               :workdir (default-workdir)
               :base-port 10080
               :time-limit 30
               :concurrency 10
               :nemesis-mode "none"
               :nemesis-interval 5
               :node-count 5
               :nodes (node-names 5)
               :key "k"}
         remaining args]
    (if (empty? remaining)
      opts
      (let [[flag value & rest] remaining]
        (case flag
          "--jar" (recur (assoc opts :jar-path (.getCanonicalPath (io/file value))) rest)
          "--time-limit" (recur (assoc opts :time-limit (parse-long-arg value)) rest)
          "--concurrency" (recur (assoc opts :concurrency (parse-long-arg value)) rest)
          "--base-port" (recur (assoc opts :base-port (parse-long-arg value)) rest)
          "--node-count" (let [count (parse-long-arg value)]
                           (recur (assoc opts
                                         :node-count count
                                         :nodes (node-names count))
                                  rest))
          "--nemesis" (recur (assoc opts :nemesis-mode value) rest)
          "--nemesis-interval" (recur (assoc opts :nemesis-interval (parse-long-arg value)) rest)
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
    "  --time-limit <sec>    Workload duration, default 30"
    "  --concurrency <n>     Client concurrency, default 10"
    "  --base-port <port>    First node port, default 10080"
    "  --node-count <n>      Number of local nodes, default 5"
    "  --nemesis <mode>      none|crash-restart|partition-one|partition-leader|partition-leader-minority|membership-join-promote|membership-demote|membership-remove-follower, default none"
    "  --nemesis-interval <sec> Nemesis interval, default 5"
    "  --workdir <path>      Local work directory, default ./work"]))

(def cas-values ["v0" "v1" "v2" "v3" "v4"])

(defn- random-value []
  (rand-nth cas-values))

(defn- random-expected []
  (rand-nth (vec (cons nil cas-values))))

(defn- write-op [_ _]
  {:type :invoke
   :f :write
   :value (random-value)})

(defn- read-op [_ _]
  {:type :invoke
   :f :read})

(defn- cas-op [_ _]
  (let [expected (random-expected)
        next-value (loop [candidate (random-value)]
                     (if (= candidate expected)
                       (recur (random-value))
                       candidate))]
    {:type :invoke
     :f :cas
     :value [expected next-value]}))

(defn- filter-history-checker [inner-checker pred]
  (reify
    checker/Checker
    (check [_ test history opts]
      (checker/check inner-checker test (filterv pred history) opts))))

(defn- workload [opts]
  {:checker (checker/compose
             {:linearizable (filter-history-checker
                             (checker/linearizable {:model (model/cas-register)})
                             #(not= :nemesis (:process %)))
              :timeline (timeline/html)})
   :generator (->> (gen/mix [write-op read-op cas-op])
                   (gen/limit 100))})

(defn- nemesis-object [opts]
  (case (:nemesis-mode opts)
    "none" nemesis/noop
    "crash-restart" (raft-nemesis/crash-restart)
    "partition-one" (raft-nemesis/partition-one)
    "partition-leader" (raft-nemesis/partition-leader)
    "partition-leader-minority" (raft-nemesis/partition-leader-minority)
    "membership-join-promote" (raft-nemesis/membership-join-promote)
    "membership-demote" (raft-nemesis/membership-demote)
    "membership-remove-follower" (raft-nemesis/membership-remove-follower)
    (throw (ex-info "Unknown nemesis mode" {:nemesis-mode (:nemesis-mode opts)}))))

(defn- nemesis-generator [opts]
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
    (gen/cycle
     (gen/phases
      (gen/once {:f :stop})
      (gen/sleep (:nemesis-interval opts))
      (gen/once {:f :start})
      (gen/sleep (:nemesis-interval opts))))))

(defn- generator [opts]
  (let [client-gen (:generator (workload opts))
        base (->> client-gen
                  gen/clients
                  (gen/stagger 1/10))
        with-nemesis (if-let [n-gen (nemesis-generator opts)]
                       (->> base
                            (gen/nemesis n-gen))
                       base)]
    (gen/phases
     (gen/time-limit (:time-limit opts) with-nemesis)
     (gen/log "Waiting for cluster to settle")
     (gen/sleep 5)
     (gen/clients
      (gen/each-thread
       (gen/once {:f :read :value nil}))))))

(defn raft-test [opts]
  (merge
   tests/noop-test
   (workload opts)
   {:name (str "raft-kv-local-" (:node-count opts) "n")
    :os os/noop
    :remote local-remote/remote
    :nodes (:nodes opts)
    :db (raft-db/local-db)
    :client (raft-client/client opts)
    :nemesis (nemesis-object opts)
    :generator (generator opts)
    :base-port (:base-port opts)
    :node-count (:node-count opts)
    :time-limit (:time-limit opts)
    :concurrency (:concurrency opts)
    :repo-root (:repo-root opts)
    :workdir (:workdir opts)
    :jar-path (:jar-path opts)
    :key (:key opts)}))

(defn -main [& args]
  (let [opts (parse-args args)]
    (if (:help opts)
      (println (usage))
      (do
        (let [opts (assoc opts :jar-path (raft-db/resolved-jar-path opts))]
          (println "Running local Jepsen harness with options:" (pr-str (dissoc opts :help)))
          (jepsen/run! (raft-test opts)))))))
