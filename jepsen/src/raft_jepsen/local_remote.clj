(ns raft-jepsen.local-remote
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [jepsen.control.core :as control])
  (:import (java.nio.file Files CopyOption StandardCopyOption)
           (java.io File)))

(defn- ensure-parent! [path]
  (when-let [parent (.getParentFile (io/file path))]
    (.mkdirs parent)))

(defn- shell-command [context cmd]
  ;; Jepsen's control layer is designed for SSH remotes. This harness runs all
  ;; nodes locally, so commands are executed through /bin/sh with the same :dir
  ;; semantics Jepsen would provide on a remote host.
  (let [dir (:dir context)
        wrapped (if dir
                  (str "cd " (control/escape dir) " && " cmd)
                  cmd)]
    ["/bin/sh" "-lc" wrapped]))

(defrecord LocalRemote [host]
  ;; LocalRemote implements the subset of jepsen.control Remote needed by this
  ;; harness: execute shell commands, upload files, and download logs. That lets
  ;; jepsen/run! use normal Jepsen plumbing without requiring SSH or containers.
  control/Remote
  (connect [this conn-spec]
    (assoc this :host (:host conn-spec)))
  (disconnect! [this]
    this)
  (execute! [this context action]
    (let [command (shell-command context (:cmd action))
          {:keys [exit out err]}
          (apply sh/sh
                 (concat command
                         (when-let [in (:in action)]
                           [:in in])))]
      (assoc action
             :host host
             :exit exit
             :out out
             :err err)))
  (upload! [this _context local-paths remote-path _opts]
    (doseq [local-path (flatten [local-paths])]
      (let [source (.toPath (io/file local-path))
            destination (.toPath (io/file remote-path))]
        (ensure-parent! remote-path)
        (Files/copy source destination
                    (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))))
  (download! [this _context remote-paths local-path _opts]
    (doseq [remote-path (flatten [remote-paths])]
      (let [source (.toPath (io/file remote-path))
            target-file (let [target (io/file local-path)]
                          (if (.isDirectory target)
                            (io/file target (.getName (io/file remote-path)))
                            target))
            destination (.toPath target-file)]
        (ensure-parent! (.getPath target-file))
        (Files/copy source destination
                    (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))))))

(def remote
  (->LocalRemote nil))
