(ns raft-jepsen.client
  (:require [cheshire.core :as json]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.client :as client]
            [raft-jepsen.db :as raft-db]
            [raft-jepsen.observer :as observer]))

(defn- parse-json [text]
  (when-let [trimmed (some-> text str/trim not-empty)]
    (json/parse-string trimmed true)))

(defn- parse-bool [value]
  (case (some-> value str/lower-case)
    "true" true
    "false" false
    nil))

(defn- parse-cpp-lines [text]
  (let [raw (into {}
                  (keep (fn [line]
                          (when-let [[_ key value] (re-matches #"([^:]+):\s*(.*)" line)]
                            [(keyword key) value])))
                  (str/split-lines (or text "")))
        result (cond
                 (contains? raw :found)
                 {:found (parse-bool (:found raw))
                  :value (:value raw)}

                 (contains? raw :cas.matched)
                 {:key (:cas.key raw)
                  :matched (parse-bool (:cas.matched raw))
                  :expectedPresent (parse-bool (:cas.expected_present raw))
                  :expectedValue (:cas.expected_value raw)
                  :newValue (:cas.new_value raw)
                  :currentPresent (parse-bool (:cas.current_present raw))
                  :currentValue (:cas.current_value raw)}

                 :else nil)]
    (cond-> {:peerId (:peer_id raw)
             :status (:status raw)
             :success (parse-bool (:success raw))
             :leaderId (:leader_id raw)
             :leaderHost (:leader_host raw)
             :leaderPort (some-> (:leader_port raw) parse-long)
             :message (:message raw)}
      result (assoc :result result))))

(defn- java-command [jar-path & args]
  (vec (concat ["java" "-jar" jar-path] args)))

(defn- cpp-command [cpp-bin & args]
  (vec (concat [cpp-bin] args)))

(defn- client-impl-for-node [test node]
  (case (get test :client-impl :java)
    :java :java
    :cpp :cpp
    :mixed (raft-db/node-impl test node)))

(defn- run-command [repo-root command]
  (apply sh/sh (concat command [:dir repo-root])))

(defn- classify-write [op exit response key value]
  (let [status (:status response)
        success? (and (zero? exit) (#{"ACCEPTED" "OK"} status) (:success response))]
    (cond
      success? (assoc op :type :ok :value value)
      (#{"RETRY" "REDIRECT" "NOT_LEADER"} status) (assoc op :type :fail :error status)
      (#{"UNREACHABLE" "TIMEOUT"} status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error (or status :command-failed)))))

(defn- classify-read [op exit response key]
  (let [status (:status response)
        success? (and (zero? exit) (= "OK" status) (:success response))
        value (when success?
                (let [result (:result response)]
                  (when (:found result)
                    (:value result))))]
    (cond
      success? (assoc op :type :ok :value value)
      (#{"RETRY" "REDIRECT" "NOT_LEADER"} status) (assoc op :type :fail :error status)
      (#{"UNREACHABLE" "TIMEOUT"} status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error (or status :query-failed)))))

(defn- classify-cas [op exit response]
  (let [status (:status response)
        success? (and (zero? exit) (#{"ACCEPTED" "OK"} status) (:success response))
        result (:result response)
        matched? (= true (:matched result))]
    (cond
      (and success? matched?) (assoc op :type :ok)
      (and success? (contains? result :matched)) (assoc op :type :fail :error :cas-mismatch)
      (#{"RETRY" "REDIRECT" "NOT_LEADER"} status) (assoc op :type :fail :error status)
      (#{"UNREACHABLE" "TIMEOUT"} status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error (or status :command-failed)))))

(defn- maybe-capture! [test node op result]
  (when (#{:fail :info} (:type result))
    (observer/capture-safe! test "client-anomaly"
                            {:node node
                             :op {:f (:f op)
                                  :value (:value op)
                                  :error (:error result)
                                  :type (:type result)}
                             :extra {:rawResponse (:raw-response result)}})))

(defrecord RaftCliClient [node repo-root jar-path cpp-bin]
  client/Client
  (open! [this _test node]
    (assoc this :node node))
  (setup! [this _test]
    this)
  (invoke! [this test op]
    (let [target (raft-db/peer-spec test node)
          host "127.0.0.1"
          port (str (raft-db/node-port test node))
          impl (client-impl-for-node test node)
          key (or (:key op) (:key test))
          cli-key (str key)
          value (some-> (:value op) str)]
      (try
        (case (:f op)
          :write (let [{:keys [exit out err]}
                       (run-command repo-root
                                    (case impl
                                      :java (java-command jar-path "command" "--json" "put" target cli-key value)
                                      :cpp (cpp-command cpp-bin "client-put" host port cli-key value "jepsen-cpp-client")))
                       response (or (case impl
                                      :java (parse-json out)
                                      :cpp (parse-cpp-lines out))
                                    {})]
                   (let [result (-> op
                                    (classify-write exit response key value)
                                    (assoc :raw-response response :stderr err))]
                     (maybe-capture! test node op result)
                     result))
          :read (let [{:keys [exit out err]}
                      (run-command repo-root
                                   (case impl
                                     :java (java-command jar-path "query" "--json" "get" target cli-key)
                                     :cpp (cpp-command cpp-bin "client-get" host port cli-key "jepsen-cpp-client")))
                      response (or (case impl
                                     :java (parse-json out)
                                     :cpp (parse-cpp-lines out))
                                   {})]
                  (let [result (-> op
                                    (classify-read exit response key)
                                    (assoc :raw-response response :stderr err))]
                    (maybe-capture! test node op result)
                    result))
          :cas (let [[expected new-value] (:value op)
                     expected-arg (if (nil? expected) "__nil__" (str expected))
                     expected-present (if (nil? expected) "false" "true")
                     expected-value (if (nil? expected) "" (str expected))
                     new-value (str new-value)
                     {:keys [exit out err]}
                     (run-command repo-root
                                  (case impl
                                    :java (java-command jar-path "command" "--json" "cas" target cli-key expected-arg new-value)
                                    :cpp (cpp-command cpp-bin "client-cas" host port cli-key expected-present expected-value new-value "jepsen-cpp-client")))
                     response (or (case impl
                                    :java (parse-json out)
                                    :cpp (parse-cpp-lines out))
                                  {})]
                 (let [result (-> op
                                  (classify-cas exit response)
                                  (assoc :raw-response response :stderr err))]
                   (maybe-capture! test node op result)
                   result))
          (assoc op :type :fail :error :unsupported-operation))
        (catch Throwable t
          (let [result (assoc op :type :info :error (.getMessage t))]
            (maybe-capture! test node op result)
            result)))))
  (teardown! [this _test]
    this)
  (close! [this _test]
    this))

(defn client [opts]
  (->RaftCliClient nil
                   (:repo-root opts)
                   (raft-db/resolved-jar-path opts)
                   (when (#{:cpp :mixed} (:client-impl opts))
                     (raft-db/resolved-cpp-bin opts))))
