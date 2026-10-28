(ns raft-jepsen.db-test
  (:require [clojure.test :refer [deftest is run-tests use-fixtures]]
            [raft-jepsen.db :as db]
            [raft-jepsen.nemesis :as nemesis]
            [raft-jepsen.client :as client]
            [raft-jepsen.core :as core]
            [jepsen.checker :as checker]))

(use-fixtures :each
  (fn [test]
    (let [processes @db/processes
          docker-state @db/docker-state]
      (try
        (reset! db/processes {})
        (reset! db/docker-state {:isolated-nodes #{}})
        (test)
        (finally
          (reset! db/processes processes)
          (reset! db/docker-state docker-state))))))

(deftest local-crash-bypasses-orderly-shutdown
  (let [events (atom [])
        process (proxy [Process] []
                  (getOutputStream [] nil)
                  (getInputStream [] nil)
                  (getErrorStream [] nil)
                  (exitValue [] 0)
                  (destroy [] (swap! events conj :orderly))
                  (destroyForcibly [] (swap! events conj :abrupt) nil)
                  (waitFor ([] 0) ([_timeout _unit] true)))]
    (swap! db/processes assoc "n1" process)
    (db/crash-node! {:backend :local} "n1")
    (is (= [:abrupt] @events))
    (is (empty? @db/processes))))

(def docker-test {:backend :docker-srv :compose-project "fault-test" :nodes ["n1"]})
(def connected "{\"fault-test_raft-net\":{}}")

(deftest docker-crash-uses-sigkill
  (let [commands (atom [])]
    (with-redefs-fn {#'db/docker-compose! (fn [_ & args] (swap! commands conj args))}
      #(db/crash-node! docker-test "n1"))
    (is (= [["kill" "--signal" "SIGKILL" "n1"]] @commands))))

(deftest partition-command-failure-is-visible
  (with-redefs-fn {#'db/docker! (fn [_ command & _]
                                (if (= "inspect" command)
                                  connected
                                  (throw (ex-info "disconnect denied" {}))))}
    #(is (thrown-with-msg? clojure.lang.ExceptionInfo #"disconnect denied"
                          (db/docker-isolate-nodes! docker-test ["n1"]))))
  (is (empty? (:isolated-nodes @db/docker-state))))

(deftest partition-requires-actual-disconnection
  (with-redefs-fn {#'db/docker! (fn [_ command & _]
                                (if (= "inspect" command) connected ""))}
    #(is (thrown-with-msg? clojure.lang.ExceptionInfo #"remains connected"
                          (db/docker-isolate-nodes! docker-test ["n1"]))))
  (is (empty? (:isolated-nodes @db/docker-state))))

(deftest healing-requires-actual-reconnection
  (swap! db/docker-state assoc :isolated-nodes #{"n1"})
  (with-redefs-fn {#'db/docker! (fn [_ command & _]
                                (if (= "inspect" command) "{}" ""))}
    #(is (thrown-with-msg? clojure.lang.ExceptionInfo #"remains disconnected"
                          (db/docker-heal-isolation! docker-test {:nodes ["n1"]}))))
  (is (= #{"n1"} (:isolated-nodes @db/docker-state))))

(deftest healing-is-idempotent
  (swap! db/docker-state assoc :isolated-nodes #{"n1"})
  (with-redefs-fn {#'db/docker! (fn [_ command & _]
                                (is (= "inspect" command))
                                connected)}
    #(db/docker-heal-isolation! docker-test {:nodes ["n1"]}))
  (is (empty? (:isolated-nodes @db/docker-state))))

(deftest promotion-rediscovers-only-after-definite-non-admission
  (let [attempts (atom 0)]
    (with-redefs-fn {#'nemesis/shell! (fn [& _]
                                      (if (= 1 (swap! attempts inc))
                                        {:exit 2 :out "status=REJECTED Node is not leader or reconfiguration could not be applied"}
                                        {:exit 0 :out "status=ACCEPTED"}))
                    #'nemesis/leader-node (constantly "n2")}
      #(is (= "status=ACCEPTED" (#'nemesis/promote-member! {:base-port 10000 :nodes ["n1" "n2" "n3" "n4"]} "n1" "n4"))))
    (is (= 2 @attempts)))
  (doseq [response ["status=TIMEOUT" "status=RETRY" "status=REJECTED joint proposal failed"]]
    (let [attempts (atom 0)]
      (with-redefs-fn {#'nemesis/shell! (fn [& _] (swap! attempts inc) {:exit 2 :out response})}
        #(is (thrown? clojure.lang.ExceptionInfo
                      (#'nemesis/promote-member! {:base-port 10000 :nodes ["n1" "n4"]} "n1" "n4"))))
      (is (= 1 @attempts)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'raft-jepsen.db-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))

(deftest missing-command-confirmation-is-an-uncertain-operation
  (is (= :info (:type (#'client/classify-write {:f :write} 1 {} "k" "v"))))
  (is (= :info (:type (#'client/classify-write {:f :write} 0 {:status "RETRY"} "k" "v"))))
  (is (= :info (:type (#'client/classify-cas {:f :cas} 0 {:status "RETRY"}))))
  (is (= :info (:type (#'client/classify-cas {:f :cas} 0 {:status "ACCEPTED" :success true}))))
  (is (= :fail (:type (#'client/classify-cas {:f :cas} 0 {:status "ACCEPTED" :success true :result {:matched false}})))))

(deftest docker-java-client-resolves-redirects-inside-its-network
  (let [captured (atom nil)
        test {:backend :docker-srv :node-impls {"raft-1" :java}}]
    (with-redefs-fn {#'client/run-command (fn [_ command] (reset! captured command))}
      #(do
         (#'client/run-client-command test "raft-1" :java "." ["java" "-jar" "/host/raft.jar" "query" "--json" "get" "raft-1@127.0.0.1:7000" "k"])
         (is (= ["docker" "exec" "raft-1" "java" "-jar" "/app/raft.jar" "query" "--json" "get" "raft-1@127.0.0.1:7000" "k"] @captured))
         (#'client/run-client-command {:backend :local} "n1" :java "." ["java" "-jar" "/host/raft.jar"])
         (is (= ["java" "-jar" "/host/raft.jar"] @captured))))))

(deftest workload-requires-successful-mutations-and-reads
  (let [progress (get-in (#'core/workload {}) [:checker :checkers :progress])
        read {:process 0 :type :ok :f :read}
        write {:process 1 :type :ok :f :write}]
    (is (false? (:valid? (checker/check progress {} [] {}))))
    (is (false? (:valid? (checker/check progress {} [read] {}))))
    (is (true? (:valid? (checker/check progress {} [read write] {}))))))
