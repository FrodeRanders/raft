(ns raft-jepsen.client
  (:require [cheshire.core :as json]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.client :as client]
            [raft-jepsen.db :as raft-db]
            [raft-jepsen.observer :as observer]))

(defn- parse-json [text]
  ;; Java CLI commands emit JSON, which maps cleanly into Clojure maps that the
  ;; classifier functions can inspect.
  (when-let [trimmed (some-> text str/trim not-empty)]
    (json/parse-string trimmed true)))

(defn- parse-bool [value]
  (case (some-> value str/lower-case)
    "true" true
    "false" false
    nil))

(defn- parse-cpp-lines [text]
  ;; The C++ smoke CLI emits stable "key: value" lines instead of JSON. This
  ;; adapter normalizes that output to the same shape as the Java JSON CLI so
  ;; the rest of the Jepsen client can classify both implementations uniformly.
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

(defn- rust-command [rust-bin & args]
  (vec (concat [rust-bin] args)))

(defn- client-impl-for-node [test node]
  ;; In Jepsen, each client instance is opened against one logical node. The
  ;; :mixed mode uses that target node's implementation to choose the CLI,
  ;; which lets one run exercise both Java and C++ client surfaces.
  (case (get test :client-impl :java)
    :java :java
    :cpp :cpp
    :rust :rust
    :mixed (raft-db/node-impl test node)))

(defn- run-command [repo-root command]
  (apply sh/sh (concat command [:dir repo-root])))

(defn- classify-write [op exit response key value]
  ;; invoke! must return a completed operation map. :ok operations become part
  ;; of the linearizability history. :fail operations did not take effect from
  ;; the client's perspective. :info means uncertain; Knossos may consider both
  ;; possibilities when checking the history.
  (let [status (:status response)
        success? (and (zero? exit) (= "ACCEPTED" status) (:success response))]
    (cond
      success? (assoc op :type :ok :value value)
      (#{"RETRY" "REDIRECT" "NOT_LEADER"} status) (assoc op :type :fail :error status)
      (#{"UNREACHABLE" "TIMEOUT"} status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error (or status :command-failed)))))

(defn- classify-read [op exit response key]
  ;; Reads return :ok with the observed value. A missing key is represented as
  ;; nil, matching the CAS register model's initial state.
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
  ;; A failed CAS due to value mismatch is a definite :fail, not an error in
  ;; the system. It tells the checker that the compare-and-set did not update
  ;; the register.
  (let [status (:status response)
        success? (and (zero? exit) (= "ACCEPTED" status) (:success response))
        result (:result response)
        matched? (= true (:matched result))]
    (cond
      (and success? matched?) (assoc op :type :ok)
      (and success? (contains? result :matched)) (assoc op :type :fail :error :cas-mismatch)
      (#{"RETRY" "REDIRECT" "NOT_LEADER"} status) (assoc op :type :fail :error status)
      (#{"UNREACHABLE" "TIMEOUT"} status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error (or status :command-failed)))))

(defn- maybe-capture! [test node op result]
  ;; Jepsen histories contain operation outcomes, but root-causing failures is
  ;; easier with extra cluster state. This hook snapshots telemetry on failed
  ;; or uncertain client operations without changing the operation result.
  (when (#{:fail :info} (:type result))
    (observer/capture-safe! test "client-anomaly"
                            {:node node
                             :op {:f (:f op)
                                  :value (:value op)
                                  :error (:error result)
                                  :type (:type result)}
                             :extra {:rawResponse (:raw-response result)}})))

(defrecord RaftCliClient [node repo-root jar-path cpp-bin rust-bin]
  ;; Client is Jepsen's per-worker handle for driving the system. Jepsen calls:
  ;; open! once per worker/node pairing, setup! before the run, invoke! for each
  ;; generated operation, and teardown!/close! at the end.
  client/Client
  (open! [this _test node]
    (assoc this :node node))
  (setup! [this _test]
    this)
  (invoke! [this test op]
    ;; The op's :f selects the workload action generated in core.clj. The
    ;; client translates that abstract operation into a concrete CLI command,
    ;; parses the response, then classifies it back into Jepsen history terms.
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
                                       :cpp (cpp-command cpp-bin "client-put" host port cli-key value "jepsen-cpp-client")
                                       :rust (rust-command rust-bin "client-put" host port cli-key value "jepsen-rust-client")))
                        response (or (case impl
                                       :java (parse-json out)
                                       (:cpp :rust) (parse-cpp-lines out))
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
                                      :cpp (cpp-command cpp-bin "client-get" host port cli-key "jepsen-cpp-client")
                                      :rust (rust-command rust-bin "client-get" host port cli-key "jepsen-rust-client")))
                       response (or (case impl
                                      :java (parse-json out)
                                      (:cpp :rust) (parse-cpp-lines out))
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
                                     :cpp (cpp-command cpp-bin "client-cas" host port cli-key expected-present expected-value new-value "jepsen-cpp-client")
                                     :rust (rust-command rust-bin "client-cas" host port cli-key expected-present expected-value new-value "jepsen-rust-client")))
                      response (or (case impl
                                     :java (parse-json out)
                                     (:cpp :rust) (parse-cpp-lines out))
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
  ;; Construct the prototype client stored in the test map. Jepsen clones it
  ;; through open!, so mutable per-worker state should be attached there rather
  ;; than hidden globally.
  (->RaftCliClient nil
                    (:repo-root opts)
                    (raft-db/resolved-jar-path opts)
                    (when (#{:cpp :mixed} (:client-impl opts))
                      (raft-db/resolved-cpp-bin opts))
                    (when (#{:rust :mixed} (:client-impl opts))
                      (raft-db/resolved-rust-bin opts))))
