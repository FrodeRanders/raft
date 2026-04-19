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

(defn- parse-status [text]
  (some->> text
           (re-find #"status=([A-Z_]+)")
           second))

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
  (let [known-nodes (conj (vec (:nodes test)) (raft-db/membership-node test))]
    (some (fn [node]
            (try
              (when-let [response (cluster-summary test node)]
                (let [leader-id (some-> (:leaderId response) str/trim not-empty)]
                  (when (and (:success response)
                             leader-id
                             (some #(= leader-id %) known-nodes))
                    leader-id)))
              (catch Throwable _
                nil)))
          known-nodes)))

(defn- member-summary [summary node]
  (some #(when (= node (:peerId %)) %) (:members summary)))

(defn- stable-learner-state [summary member]
  (let [member-state (member-summary summary member)]
    (when (and (:success summary)
               (not (:jointConsensus summary))
               member-state
               (= "LEARNER" (:role member-state))
               (= "LEARNER" (:currentRole member-state))
               (= "steady" (:roleTransition member-state)))
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

(defn- demote-member! [test leader member]
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
                                            learner-state (stable-learner-state summary member)
                                            join-state (join-status test leader member)]
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
