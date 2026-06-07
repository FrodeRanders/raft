(ns raft-jepsen.nemesis
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.nemesis]
            [raft-jepsen.db :as raft-db]
            [raft-jepsen.observer :as observer]))

(declare leader-node)

(defn- random-node
  ;; Nemeses choose targets from Jepsen's logical :nodes list. Keeping target
  ;; choice here, rather than in the generator, lets the same generator drive
  ;; several fault types.
  ([nodes]
   (when (seq nodes)
     (rand-nth (vec nodes))))
  ([test nodes]
   (random-node nodes)))

(defn crash-restart []
  ;; A Nemesis is like a client for faults. Its invoke! receives operations
  ;; emitted by the nemesis generator, typically {:f :stop} and {:f :start}.
  ;; It returns :info operations because faults are observations in the history,
  ;; not register operations checked for linearizability.
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

(defn- follower-node [test]
  (when-let [leader (leader-node test)]
    (first (remove #(= leader %) (:nodes test)))))

(defn persistence-loss-restart []
  ;; This variant restarts a follower after deleting its data directory. The
  ;; follower choice avoids intentionally destroying the current leader's local
  ;; state, so the scenario focuses on catch-up/recovery rather than immediate
  ;; quorum disruption.
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
                       (observer/capture-safe! test "nemesis-persistence-loss-restart"
                                               {:node node
                                                :op {:f :start}
                                                :extra {:phase :restarted-fresh}})
                       (reset! disrupted-node nil)
                       (assoc op :type :info :value {node :restarted-fresh}))
                     (assoc op :type :info :value :not-started))
            :stop (if @disrupted-node
                    (assoc op :type :info :value :already-stopped)
                    (if-let [node (follower-node test)]
                      (do
                        (raft-db/wipe-node-data! test node)
                        (observer/capture-safe! test "nemesis-persistence-loss-restart"
                                                {:node node
                                                 :op {:f :stop}
                                                 :extra {:phase :data-wiped}})
                        (reset! disrupted-node node)
                        (assoc op :type :info :value {node :data-wiped}))
                      (assoc op :type :info :value :no-target)))
            (assoc op :type :info :error :unsupported-nemesis-op))))
      (teardown! [_ test]
        (when-let [node @disrupted-node]
          (raft-db/start-node! test node true)
          (reset! disrupted-node nil))))))

(defn process-pause []
  ;; Pause/resume models stop-the-world behavior: the process remains alive and
  ;; keeps its memory and sockets, but it stops making progress. This is closer
  ;; to long JVM GC pauses or scheduler starvation than crash/restart.
  (let [paused-node (atom nil)]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (locking paused-node
          (case (:f op)
            :start (if-let [node @paused-node]
                     (do
                       (raft-db/resume-node! test node)
                       (observer/capture-safe! test "nemesis-process-pause" {:node node :op {:f :start}})
                       (reset! paused-node nil)
                       (assoc op :type :info :value {node :resumed}))
                     (assoc op :type :info :value :not-paused))
            :stop (if @paused-node
                    (assoc op :type :info :value :already-paused)
                    (if-let [node (random-node test (:nodes test))]
                      (do
                        (observer/capture-safe! test "nemesis-process-pause" {:node node :op {:f :stop}})
                        (raft-db/pause-node! test node)
                        (reset! paused-node node)
                        (assoc op :type :info :value {node :paused}))
                      (assoc op :type :info :value :no-target)))
            (assoc op :type :info :error :unsupported-nemesis-op))))
      (teardown! [_ test]
        (when-let [node @paused-node]
          (raft-db/resume-node! test node)
          (reset! paused-node nil))))))

(defn clock-skew []
  ;; Clock skew is implemented through the Raft logical clock hook. The node is
  ;; restarted with a configured offset so leases/election deadlines experience
  ;; skew without changing the host clock for the whole test machine.
  (let [skewed-node (atom nil)]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (locking skewed-node
          (case (:f op)
            :start (if-let [node @skewed-node]
                     (do
                       (raft-db/clear-node-clock-offset! test node)
                       (observer/capture-safe! test "nemesis-clock-skew" {:node node :op {:f :start}})
                       (reset! skewed-node nil)
                       (assoc op :type :info :value {node :clock-restored}))
                     (assoc op :type :info :value :not-skewed))
            :stop (if @skewed-node
                    (assoc op :type :info :value :already-skewed)
                    (if-let [node (random-node test (:nodes test))]
                      (let [offset (long (get test :clock-skew-millis 5000))]
                        (observer/capture-safe! test "nemesis-clock-skew"
                                                {:node node
                                                 :op {:f :stop}
                                                 :extra {:clockOffsetMillis offset}})
                        (raft-db/set-node-clock-offset! test node offset)
                        (reset! skewed-node node)
                        (assoc op :type :info :value {node {:clock-offset-millis offset}}))
                      (assoc op :type :info :value :no-target)))
            (assoc op :type :info :error :unsupported-nemesis-op))))
      (teardown! [_ test]
        (when-let [node @skewed-node]
          (raft-db/clear-node-clock-offset! test node)
          (reset! skewed-node nil))))))

(defn- partition-script [test]
  (.getAbsolutePath (io/file (:repo-root test) "jepsen" "scripts" "partition.sh")))

(defn- run-script! [test & args]
  ;; Network partitions are delegated to a small shell helper because packet
  ;; filtering is OS-specific and may require sudo. The nemesis raises if the
  ;; helper fails so the Jepsen run does not silently continue without a fault.
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

(defn- parse-status [text]
  (some->> text
           (re-find #"status=([A-Z_]+)")
           second))

(defn- parse-bool [value]
  (case (some-> value str/lower-case)
    "true" true
    "false" false
    nil))

(defn- parse-cpp-summary [text]
  (let [raw (into {}
                  (keep (fn [line]
                          (when-let [[_ key value] (re-matches #"([^:]+):\s*(.*)" line)]
                            [(keyword key) value])))
                  (str/split-lines (or text "")))]
    {:peerId (:peer_id raw)
     :status (:status raw)
     :success (parse-bool (:success raw))
     :leaderId (:leader_id raw)
     :clusterHealth (:cluster_health raw)}))

(defn- target-spec [test node]
  (str node "@127.0.0.1:" (raft-db/node-port test node)))

(defn- cluster-summary [test node]
  ;; Nemeses often need current cluster state, for example to locate the leader
  ;; before isolating it. These probes are outside the checked workload; they
  ;; are control-plane observations used to aim faults.
  (if (raft-db/docker-backend? test)
    (let [{:keys [exit out err]}
          (shell! (:repo-root test)
                  [(raft-db/resolved-cpp-bin test)
                   "cluster-summary"
                   "127.0.0.1"
                   (str (raft-db/node-port test node))
                   "jepsen-control"])]
      (when-not (zero? exit)
        (throw (ex-info "Docker/SRV cluster summary command failed"
                        {:node node :exit exit :out out :err err})))
      (parse-cpp-summary out))
    (let [{:keys [exit out err]}
          (shell! (:repo-root test)
                  (java-command (:jar-path test) "cluster-summary" "--json" (target-spec test node)))
          response (parse-json out)]
      (when-not (zero? exit)
        (throw (ex-info "Cluster summary command failed"
                        {:node node :exit exit :out out :err err})))
      response)))

(defn- telemetry-summary [test node]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test) "telemetry" "--json" "--summary" (target-spec test node)))
        response (parse-json out)]
    (when-not (zero? exit)
      (throw (ex-info "Telemetry summary command failed"
                      {:node node :exit exit :out out :err err})))
    response))

(defn- reconfiguration-status [test node]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test) "reconfiguration-status" "--json" (target-spec test node)))
        response (parse-json out)]
    (when-not (zero? exit)
      (throw (ex-info "Reconfiguration status command failed"
                      {:node node :exit exit :out out :err err})))
    response))

(defn- join-status [test node joining-peer-id]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test) "join-status" (target-spec test node) joining-peer-id))
        status (parse-status out)]
    (when-not (zero? exit)
      (throw (ex-info "Join status command failed"
                      {:node node :joining-peer-id joining-peer-id :exit exit :out out :err err})))
    {:status status
     :stdout out}))

(defn- join-request! [test node joining-peer-id]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test)
                              "join-request"
                              (target-spec test node)
                              (str (target-spec test joining-peer-id) "/learner")))
        status (parse-status out)]
    (when-not (zero? exit)
      (throw (ex-info "Join request command failed"
                      {:node node :joining-peer-id joining-peer-id :exit exit :out out :err err})))
    {:status status
     :stdout out}))

(defn- leader-node [test]
  ;; Leader discovery tries every known node and accepts the first successful
  ;; summary that names a valid peer. During elections or partitions this may
  ;; temporarily return nil, which callers treat as "no safe target yet".
  (let [known-nodes (conj (vec (:nodes test)) (raft-db/membership-node test))]
    (some (fn [node]
            (try
              (when-let [response (cluster-summary test node)]
                (let [leader-id (some-> (:leaderId response) str/trim not-empty)]
                  (when (and leader-id
                             (some #(= leader-id %) known-nodes))
                    leader-id)))
              (catch Throwable _
                nil)))
          known-nodes)))

(defn- member-summary [summary node]
  (some #(when (= node (:peerId %)) %) (:members summary)))

(defn- stable-learner-state [summary join-state member]
  ;; Membership nemeses wait for externally visible stable states instead of
  ;; assuming a reconfiguration command has taken effect when the CLI returns.
  ;; That keeps the generated fault event aligned with the cluster state the
  ;; workload actually experiences.
  (let [member-state (member-summary summary member)]
    (when (and (:success summary)
               (not (:jointConsensus summary))
               member-state
               (= "LEARNER" (:role member-state))
               (:reachable member-state)
               (= "healthy" (:health member-state))
               (zero? (:lag member-state))
               (or (and (= "LEARNER" (:currentRole member-state))
                        (= "steady" (:roleTransition member-state)))
                   (and (= "PENDING" (:status join-state))
                        (= "" (:currentRole member-state))
                        (= "known" (:roleTransition member-state)))))
      {:summary summary
       :member member-state})))

(defn- promotion-progress-state [summary reconfig member]
  (let [member-state (member-summary summary member)]
    (when (and (:success summary)
               (:success reconfig)
               member-state
               (= "VOTER" (:role member-state))
               (or
                (and (not (:jointConsensus summary))
                     (not (:reconfigurationActive reconfig))
                     (= "VOTER" (:currentRole member-state))
                     (= "steady" (:roleTransition member-state)))
                (and (:jointConsensus summary)
                     (= "LEARNER" (:currentRole member-state))
                     (= "VOTER" (:nextRole member-state))
                     (= "promoting" (:roleTransition member-state)))))
      {:summary summary
       :reconfig reconfig
       :member member-state})))

(defn- demotion-progress-state [summary reconfig member]
  (let [member-state (member-summary summary member)]
    (when (and (:success summary)
               (:success reconfig)
               member-state
               (= "LEARNER" (:role member-state))
               (or
                (and (not (:jointConsensus summary))
                     (not (:reconfigurationActive reconfig))
                     (= "LEARNER" (:currentRole member-state))
                     (= "steady" (:roleTransition member-state)))
                (and (:jointConsensus summary)
                     (= "VOTER" (:currentRole member-state))
                     (= "LEARNER" (:nextRole member-state))
                     (= "demoting" (:roleTransition member-state)))))
      {:summary summary
       :reconfig reconfig
       :member member-state})))

(defn- removal-progress-state [summary reconfig member]
  (let [member-state (member-summary summary member)]
    (when (and (:success summary)
               (:success reconfig)
               member-state
               (:jointConsensus summary)
               (:reconfigurationActive reconfig)
               (:currentMember member-state)
               (not (:nextMember member-state)))
      {:summary summary
       :reconfig reconfig
       :member member-state})))

(defn- removed-final-state [telemetry summary member expected-voters]
  (when (and telemetry
             (not (:jointConsensus telemetry))
             (:quorumAvailable telemetry)
             (= expected-voters (:votingMembers telemetry)))
    {:telemetry telemetry
     :summary summary}))

(defn- wait-for! [timeout-ms interval-ms pred]
  ;; Polling is common in nemeses: fault setup often depends on eventual Raft
  ;; state. Returning nil on timeout lets callers attach scenario-specific
  ;; context to the exception they throw.
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if-let [value (pred)]
        value
        (when (< (System/currentTimeMillis) deadline)
          (Thread/sleep interval-ms)
          (recur))))))

(defn- target-nodes [target]
  (cond
    (nil? target) []
    (sequential? target) (vec (remove nil? target))
    :else [target]))

(defn- isolate-nodes! [test nodes]
  ;; Partition helpers work at the port level, but nemesis histories are easier
  ;; to read in node terms. Return both so logs show the logical target and the
  ;; actual ports that were filtered.
  (if (raft-db/docker-backend? test)
    (raft-db/docker-isolate-nodes! test nodes)
    (let [ports (mapv #(raft-db/node-port test %) nodes)]
      (apply run-script! test "isolate" (map str ports))
      {:nodes (vec nodes)
       :ports ports})))

(defn- heal-isolation! [test isolation]
  (when isolation
    (if (raft-db/docker-backend? test)
      (raft-db/docker-heal-isolation! test isolation)
      (apply run-script! test "heal" (map str (:ports isolation))))))

(defn- leader-minority-nodes [test]
  (when-let [leader (leader-node test)]
    (let [followers (remove #(= leader %) (:nodes test))]
      (when-let [other (random-node followers)]
        [leader other]))))

(defn- partition-nemesis [target-node-fn context-name]
  ;; A reusable partition nemesis: target-node-fn chooses the node or nodes to
  ;; isolate at :start time, and :stop heals the same isolation. The atom tracks
  ;; active state because Jepsen may call start/stop across different invokes.
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

(defn- demote-member! [test leader member]
  ;; Membership changes are injected through the public raft-dist CLI. That is
  ;; intentional: Jepsen should exercise the same administrative surface an
  ;; operator or integration test would use, not private in-process hooks.
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test)
                              "reconfigure"
                              "demote"
                              (target-spec test leader)
                              (target-spec test member)))]
    (when-not (zero? exit)
      (throw (ex-info "Demote request failed"
                      {:leader leader :member member :exit exit :out out :err err})))
    out))

(defn- joint-reconfigure! [test leader members]
  (let [member-specs (mapv #(target-spec test %) members)
        {:keys [exit out err]}
        (shell! (:repo-root test)
                (apply java-command
                       (:jar-path test)
                       "reconfigure"
                       "joint"
                       (target-spec test leader)
                       member-specs))]
    (when-not (zero? exit)
      (throw (ex-info "Joint reconfiguration request failed"
                      {:leader leader :members members :exit exit :out out :err err})))
    out))

(defn- finalize-reconfigure! [test leader]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test)
                              "reconfigure"
                              "finalize"
                              (target-spec test leader)))]
    (when-not (zero? exit)
      (throw (ex-info "Finalize reconfiguration request failed"
                      {:leader leader :exit exit :out out :err err})))
    out))

(defn- promote-member! [test leader member]
  (let [{:keys [exit out err]}
        (shell! (:repo-root test)
                (java-command (:jar-path test)
                              "reconfigure"
                              "promote"
                              (target-spec test leader)
                              (str (target-spec test member) "/learner")))]
    (when-not (zero? exit)
      (throw (ex-info "Promote request failed"
                      {:leader leader :member member :exit exit :out out :err err})))
    out))

(defn membership-join-promote []
  ;; This is a one-shot membership scenario. The generator emits one :start;
  ;; the nemesis starts an extra learner, requests admission, waits for learner
  ;; stability, promotes it, then waits for stable voter state under workload.
  (let [membership-state (atom {:started? false})]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start
          (locking membership-state
            (if (:started? @membership-state)
              (assoc op :type :info :value :already-started)
              (let [member (raft-db/membership-node test)
                    seed (raft-db/membership-seed-node test)]
                (raft-db/start-joining-node! test member false)
                (observer/capture-safe! test "nemesis-membership-join-promote"
                                        {:node member
                                         :op {:f :start}
                                         :extra {:seed seed :phase :join-started}})
                (when-let [leader (wait-for! 15000 500 #(leader-node test))]
                  (let [join-response (join-request! test leader member)]
                    (observer/capture-safe! test "nemesis-membership-join-promote"
                                            {:node member
                                             :op {:f :join-request}
                                             :extra {:leader leader
                                                     :phase :join-requested
                                                     :joinRequestStatus (:status join-response)
                                                     :joinRequestStdout (:stdout join-response)}})))
                (let [joined-summary
                      (wait-for! 30000 500
                                 #(when-let [leader (leader-node test)]
                                    (try
                                      (let [summary (cluster-summary test leader)
                                            join-state (join-status test leader member)
                                            learner-state (stable-learner-state summary join-state member)]
                                        (when learner-state
                                          {:leader leader
                                           :summary summary
                                           :member (:member learner-state)
                                           :join-state join-state}))
                                      (catch Throwable _
                                        nil))))]
                  (when-not joined-summary
                    (throw (ex-info "Timed out waiting for joining learner to stabilize"
                                    {:member member})))
                  (promote-member! test (:leader joined-summary) member)
                  (observer/capture-safe! test "nemesis-membership-join-promote"
                                          {:node member
                                           :op {:f :promote}
                                           :extra {:leader (:leader joined-summary) :phase :promotion-requested}})
                  (let [promoted-summary
                        (wait-for! 30000 500
                                   #(when-let [leader (leader-node test)]
                                      (try
                                        (let [summary (cluster-summary test leader)
                                              reconfig (reconfiguration-status test leader)
                                              progress (promotion-progress-state summary reconfig member)]
                                          (when progress
                                            {:leader leader
                                             :summary summary
                                             :reconfig reconfig
                                             :member (:member progress)}))
                                        (catch Throwable _
                                          nil))))]
                    (when-not promoted-summary
                      (throw (ex-info "Timed out waiting for promoted voter to stabilize"
                                      {:member member})))
                    (reset! membership-state {:started? true
                                              :member member
                                              :leader (:leader promoted-summary)})
                    (observer/capture-safe! test "nemesis-membership-join-promote"
                                            {:node member
                                             :op {:f :completed}
                                             :extra {:leader (:leader promoted-summary)
                                                     :phase :stable-voter}})
                    (assoc op :type :info
                           :value {:member member
                                   :leader (:leader promoted-summary)
                                   :status :stable-voter}))))))
          :stop (assoc op :type :info :value :noop)
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ test]
        (when-let [member (:member @membership-state)]
          (raft-db/stop-node! test member))))))

(defn membership-demote []
  ;; Demotion keeps the node running but removes it from the voting set. The
  ;; checker still validates client-visible register behavior while the cluster
  ;; changes quorum membership.
  (let [demotion-state (atom {:started? false})]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start
          (locking demotion-state
            (cond
              (:started? @demotion-state)
              (assoc op :type :info :value :already-started)

              :else
              (if-let [leader (leader-node test)]
                (if-let [member (first (remove #(= leader %) (:nodes test)))]
                  (do
                    (demote-member! test leader member)
                    (observer/capture-safe! test "nemesis-membership-demote"
                                            {:node member
                                             :op {:f :demote}
                                             :extra {:leader leader :phase :demotion-requested}})
                    (let [demoted-summary
                          (wait-for! 30000 500
                                     #(when-let [current-leader (leader-node test)]
                                        (try
                                          (let [summary (cluster-summary test current-leader)
                                                reconfig (reconfiguration-status test current-leader)
                                                progress (demotion-progress-state summary reconfig member)]
                                            (when progress
                                              {:leader current-leader
                                               :summary summary
                                               :reconfig reconfig
                                               :member (:member progress)}))
                                          (catch Throwable _
                                            nil))))]
                      (when-not demoted-summary
                        (throw (ex-info "Timed out waiting for demoted learner to stabilize"
                                        {:member member})))
                      (reset! demotion-state {:started? true
                                              :member member
                                              :leader (:leader demoted-summary)})
                      (observer/capture-safe! test "nemesis-membership-demote"
                                              {:node member
                                               :op {:f :completed}
                                               :extra {:leader (:leader demoted-summary)
                                                       :phase :stable-learner}})
                      (assoc op :type :info
                             :value {:member member
                                     :leader (:leader demoted-summary)
                                     :status :stable-learner})))
                  (assoc op :type :info :value :no-target))
                (assoc op :type :info :value :no-leader))))

          :stop (assoc op :type :info :value :noop)
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ _test]
        nil))))

(defn membership-remove-follower []
  ;; Removal is modeled as explicit joint consensus followed by finalize. The
  ;; nemesis waits for each phase so a failure points at the phase that got
  ;; stuck, and the history records a single high-level membership event.
  (let [removal-state (atom {:started? false})]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start
          (locking removal-state
            (cond
              (:started? @removal-state)
              (assoc op :type :info :value :already-started)

              :else
              (if-let [leader (leader-node test)]
                (if-let [member (first (remove #(= leader %) (:nodes test)))]
                  (let [remaining-members (vec (remove #(= member %) (:nodes test)))]
                    (joint-reconfigure! test leader remaining-members)
                    (observer/capture-safe! test "nemesis-membership-remove-follower"
                                            {:node member
                                             :op {:f :joint}
                                             :extra {:leader leader
                                                     :members remaining-members
                                                     :phase :joint-requested}})
                    (let [joint-summary
                          (wait-for! 30000 500
                                     #(when-let [current-leader (leader-node test)]
                                        (try
                                          (let [summary (cluster-summary test current-leader)
                                                reconfig (reconfiguration-status test current-leader)
                                                progress (removal-progress-state summary reconfig member)]
                                            (when progress
                                              {:leader current-leader
                                               :summary summary
                                               :reconfig reconfig
                                               :member (:member progress)}))
                                          (catch Throwable _
                                            nil))))]
                      (when-not joint-summary
                        (throw (ex-info "Timed out waiting for removal joint configuration"
                                        {:member member :remaining-members remaining-members})))
                      (finalize-reconfigure! test (:leader joint-summary))
                      (observer/capture-safe! test "nemesis-membership-remove-follower"
                                              {:node member
                                               :op {:f :finalize}
                                               :extra {:leader (:leader joint-summary)
                                                       :members remaining-members
                                                       :phase :finalize-requested}})
                      (let [expected-voters (count remaining-members)
                            removed-summary
                            (wait-for! 30000 500
                                       #(some (fn [node]
                                                (try
                                                  (let [telemetry (telemetry-summary test node)
                                                        summary (try
                                                                  (cluster-summary test node)
                                                                  (catch Throwable _
                                                                    nil))
                                                        progress (removed-final-state telemetry
                                                                                      summary
                                                                                      member
                                                                                      expected-voters)]
                                                    (when progress
                                                      {:leader (:leaderId telemetry)
                                                       :observer node
                                                       :telemetry telemetry
                                                       :summary summary}))
                                                  (catch Throwable _
                                                    nil)))
                                              remaining-members))]
                        (when-not removed-summary
                          (throw (ex-info "Timed out waiting for follower removal to finalize"
                                          {:member member :remaining-members remaining-members})))
                        (reset! removal-state {:started? true
                                               :member member
                                               :leader (:leader removed-summary)})
                        (observer/capture-safe! test "nemesis-membership-remove-follower"
                                                {:node member
                                                 :op {:f :completed}
                                                 :extra {:leader (:leader removed-summary)
                                                         :members remaining-members
                                                         :phase :stable-removed}})
                        (assoc op :type :info
                               :value {:member member
                                       :leader (:leader removed-summary)
                                       :status :stable-removed}))))
                  (assoc op :type :info :value :no-target))
                (assoc op :type :info :value :no-leader))))

          :stop (assoc op :type :info :value :noop)
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ _test]
        nil))))

(defn membership-remove-leader []
  ;; Removing the current leader uses the same external reconfiguration flow as
  ;; follower removal, but the target is chosen as the leader to force leadership
  ;; transfer/election behavior during the workload.
  (let [removal-state (atom {:started? false})]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start
          (locking removal-state
            (cond
              (:started? @removal-state)
              (assoc op :type :info :value :already-started)

              :else
              (if-let [leader (leader-node test)]
                (let [member leader
                      remaining-members (vec (remove #(= member %) (:nodes test)))]
                  (joint-reconfigure! test leader remaining-members)
                  (observer/capture-safe! test "nemesis-membership-remove-leader"
                                          {:node member
                                           :op {:f :joint}
                                           :extra {:leader leader
                                                   :members remaining-members
                                                   :phase :joint-requested}})
                  (let [joint-summary
                        (wait-for! 30000 500
                                   #(when-let [current-leader (leader-node test)]
                                      (try
                                        (let [summary (cluster-summary test current-leader)
                                              reconfig (reconfiguration-status test current-leader)
                                              progress (removal-progress-state summary reconfig member)]
                                          (when progress
                                            {:leader current-leader
                                             :summary summary
                                             :reconfig reconfig
                                             :member (:member progress)}))
                                        (catch Throwable _
                                          nil))))]
                    (when-not joint-summary
                      (throw (ex-info "Timed out waiting for leader removal joint configuration"
                                      {:member member :remaining-members remaining-members})))
                    (finalize-reconfigure! test (:leader joint-summary))
                    (observer/capture-safe! test "nemesis-membership-remove-leader"
                                            {:node member
                                             :op {:f :finalize}
                                             :extra {:leader (:leader joint-summary)
                                                     :members remaining-members
                                                     :phase :finalize-requested}})
                    (let [expected-voters (count remaining-members)
                          removed-summary
                          (wait-for! 30000 500
                                     #(some (fn [node]
                                              (try
                                                (let [telemetry (telemetry-summary test node)
                                                      summary (try
                                                                (cluster-summary test node)
                                                                (catch Throwable _
                                                                  nil))
                                                      progress (removed-final-state telemetry
                                                                                    summary
                                                                                    member
                                                                                    expected-voters)]
                                                  (when progress
                                                    {:leader (:leaderId telemetry)
                                                     :observer node
                                                     :telemetry telemetry
                                                     :summary summary}))
                                                (catch Throwable _
                                                  nil)))
                                            remaining-members))]
                      (when-not removed-summary
                        (throw (ex-info "Timed out waiting for leader removal to finalize"
                                        {:member member :remaining-members remaining-members})))
                      (reset! removal-state {:started? true
                                             :member member
                                             :leader (:leader removed-summary)})
                      (observer/capture-safe! test "nemesis-membership-remove-leader"
                                              {:node member
                                               :op {:f :completed}
                                               :extra {:leader (:leader removed-summary)
                                                       :members remaining-members
                                                       :phase :stable-removed}})
                      (assoc op :type :info
                             :value {:member member
                                     :leader (:leader removed-summary)
                                     :status :stable-removed}))))
                (assoc op :type :info :value :no-leader))))

          :stop (assoc op :type :info :value :noop)
          (assoc op :type :info :error :unsupported-nemesis-op)))
      (teardown! [_ _test]
        nil))))

(defn membership-remove-follower-partition-leader []
  ;; This compound nemesis starts a membership removal, waits until joint
  ;; consensus is visible, then partitions the leader. The matching :stop heals
  ;; the partition and tries to finalize, testing reconfiguration progress
  ;; across an overlapping network fault.
  (let [state (atom {:started? false
                     :isolation nil})]
    (reify
      jepsen.nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start
          (locking state
            (cond
              (:started? @state)
              (assoc op :type :info :value :already-started)

              :else
              (if-let [leader (leader-node test)]
                (if-let [member (first (remove #(= leader %) (:nodes test)))]
                  (let [remaining-members (vec (remove #(= member %) (:nodes test)))]
                    (joint-reconfigure! test leader remaining-members)
                    (observer/capture-safe! test "nemesis-membership-remove-follower-partition-leader"
                                            {:node member
                                             :op {:f :joint}
                                             :extra {:leader leader
                                                     :members remaining-members
                                                     :phase :joint-requested}})
                    (let [joint-summary
                          (wait-for! 30000 500
                                     #(when-let [current-leader (leader-node test)]
                                        (try
                                          (let [summary (cluster-summary test current-leader)
                                                reconfig (reconfiguration-status test current-leader)
                                                progress (removal-progress-state summary reconfig member)]
                                            (when progress
                                              {:leader current-leader
                                               :summary summary
                                               :reconfig reconfig
                                               :member (:member progress)}))
                                          (catch Throwable _
                                            nil))))]
                      (when-not joint-summary
                        (throw (ex-info "Timed out waiting for follower-removal joint configuration before partition"
                                        {:member member :remaining-members remaining-members :leader leader})))
                      (let [isolation (isolate-nodes! test [(:leader joint-summary)])]
                        (observer/capture-safe! test "nemesis-membership-remove-follower-partition-leader"
                                                {:node (:leader joint-summary)
                                                 :op {:f :partition}
                                                 :extra {:leader (:leader joint-summary)
                                                         :members remaining-members
                                                         :isolatedNodes (:nodes isolation)
                                                         :isolatedPorts (:ports isolation)
                                                         :phase :leader-partitioned}})
                        (reset! state {:started? true
                                       :member member
                                       :joint-leader (:leader joint-summary)
                                       :remaining-members remaining-members
                                       :isolation isolation})
                        (assoc op :type :info
                               :value {:member member
                                       :leader (:leader joint-summary)
                                       :isolatedLeader (:leader joint-summary)
                                       :status :joint-partitioned}))))
                  (assoc op :type :info :value :no-target))
                (assoc op :type :info :value :no-leader))))

          :stop
          (locking state
            (if-not (:started? @state)
              (assoc op :type :info :value :not-started)
              (let [{:keys [member joint-leader remaining-members isolation]} @state]
                (when isolation
                  (heal-isolation! test isolation)
                  (observer/capture-safe! test "nemesis-membership-remove-follower-partition-leader"
                                          {:node (first (:nodes isolation))
                                           :op {:f :stop}
                                           :extra {:isolatedNodes (:nodes isolation)
                                                   :isolatedPorts (:ports isolation)
                                                   :phase :leader-healed}}))
                (let [finalize-summary
                      (wait-for! 30000 500
                                 #(when-let [current-leader (leader-node test)]
                                    (try
                                      (finalize-reconfigure! test current-leader)
                                      {:leader current-leader}
                                      (catch Throwable _
                                        nil))))
                      expected-voters (count remaining-members)
                      removed-summary
                      (wait-for! 30000 500
                                 #(some (fn [node]
                                          (try
                                            (let [telemetry (telemetry-summary test node)
                                                  summary (try
                                                            (cluster-summary test node)
                                                            (catch Throwable _
                                                              nil))
                                                  progress (removed-final-state telemetry
                                                                                summary
                                                                                member
                                                                                expected-voters)]
                                              (when progress
                                                {:leader (:leaderId telemetry)
                                                 :observer node
                                                 :telemetry telemetry
                                                 :summary summary}))
                                            (catch Throwable _
                                              nil)))
                                        remaining-members))]
                  (when-not finalize-summary
                    (swap! state assoc :isolation nil)
                    (throw (ex-info "Timed out waiting for healed cluster leader after partitioned membership change"
                                    {:member member
                                     :removed-leader joint-leader
                                     :remaining-members remaining-members})))
                  (when-not removed-summary
                    (swap! state assoc :isolation nil)
                    (throw (ex-info "Timed out waiting for follower removal after healed leader partition"
                                    {:member member
                                     :removed-leader joint-leader
                                     :remaining-members remaining-members})))
                  (reset! state {:started? false
                                 :member member
                                 :leader (:leader removed-summary)
                                 :joint-leader joint-leader
                                 :remaining-members remaining-members
                                 :isolation nil})
                  (observer/capture-safe! test "nemesis-membership-remove-follower-partition-leader"
                                          {:node member
                                           :op {:f :completed}
                                           :extra {:leader (:leader removed-summary)
                                                   :isolatedLeader joint-leader
                                                   :members remaining-members
                                                   :phase :stable-removed}})
                  (assoc op :type :info
                         :value {:member member
                                 :leader (:leader removed-summary)
                                 :isolatedLeader joint-leader
                                 :status :stable-removed}))))

          (assoc op :type :info :error :unsupported-nemesis-op))))
      (teardown! [_ test]
        (when-let [isolation (:isolation @state)]
          (heal-isolation! test isolation)
          (swap! state assoc :isolation nil))))))
