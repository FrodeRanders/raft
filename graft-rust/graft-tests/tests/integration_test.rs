#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use graft_core::membership::ClusterConfiguration;
    use graft_core::raft_node::{
        AppendEntriesRequest, InstallSnapshotRequest, LogStore, RaftNode, RaftTransport,
        VoteRequest,
    };
    use graft_core::state_machine::StateMachine;
    use graft_core::types::{LogEntry, Peer};
    use graft_storage::log_store::{InMemoryLogStore, InMemoryPersistentStateStore};

    fn make_peer(id: &str, port: u16) -> Peer {
        Peer::voter(
            id.to_string(),
            format!("127.0.0.1:{}", port).parse().unwrap(),
        )
    }

    fn make_raft_node(id: &str, port: u16, peers: Vec<Peer>) -> RaftNode {
        let me = make_peer(id, port);
        let config = ClusterConfiguration::stable(peers);
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());

        RaftNode::new(me, 500, log_store, state_store, None, config, 100, 1024)
    }

    struct NoopTransport;

    impl RaftTransport for NoopTransport {
        fn send_vote_request(
            &self,
            _peer: &Peer,
            _term: u64,
            _candidate_id: &str,
            _last_log_index: u64,
            _last_log_term: u64,
        ) {
        }
        fn send_append_entries(
            &self,
            _peer: &Peer,
            _term: u64,
            _leader_id: &str,
            _prev_log_index: u64,
            _prev_log_term: u64,
            _leader_commit: u64,
            _entries: Vec<LogEntry>,
        ) {
        }
        fn send_install_snapshot(
            &self,
            _peer: &Peer,
            _term: u64,
            _leader_id: &str,
            _last_included_index: u64,
            _last_included_term: u64,
            _offset: u64,
            _data: Vec<u8>,
            _done: bool,
        ) {
        }
        fn broadcast_heartbeat_complete(&self) {}
    }

    struct RestoreTrackingStateMachine {
        restored: std::sync::Mutex<Vec<u8>>,
    }

    impl RestoreTrackingStateMachine {
        fn new() -> Self {
            Self {
                restored: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn restored(&self) -> Vec<u8> {
            self.restored.lock().unwrap().clone()
        }
    }

    impl StateMachine for RestoreTrackingStateMachine {
        fn apply(&self, _term: u64, _command: &[u8]) {}

        fn restore(&self, snapshot_data: &[u8]) {
            *self.restored.lock().unwrap() = snapshot_data.to_vec();
        }
    }

    #[test]
    fn test_initial_state_is_follower() {
        let node = make_raft_node(
            "n1",
            5001,
            vec![
                make_peer("n1", 5001),
                make_peer("n2", 5002),
                make_peer("n3", 5003),
            ],
        );
        assert_eq!(node.state, graft_core::types::NodeState::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.voted_for, None);
        assert_eq!(node.commit_index, 0);
    }

    #[test]
    fn test_vote_request_granted_when_candidate_is_up_to_date() {
        let mut node = make_raft_node(
            "n1",
            5001,
            vec![
                make_peer("n1", 5001),
                make_peer("n2", 5002),
                make_peer("n3", 5003),
            ],
        );

        let resp = node.handle_vote_request(VoteRequest {
            term: 1,
            candidate_id: "n2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(resp.vote_granted);
        assert_eq!(resp.term, 1);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some("n2".to_string()));
    }

    #[test]
    fn test_vote_request_rejected_when_term_is_lower() {
        let mut node = make_raft_node(
            "n1",
            5001,
            vec![
                make_peer("n1", 5001),
                make_peer("n2", 5002),
                make_peer("n3", 5003),
            ],
        );

        node.current_term = 5;
        let resp = node.handle_vote_request(VoteRequest {
            term: 3,
            candidate_id: "n2".to_string(),
            last_log_index: 10,
            last_log_term: 5,
        });

        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn test_vote_request_rejected_when_already_voted() {
        let mut node = make_raft_node(
            "n1",
            5001,
            vec![
                make_peer("n1", 5001),
                make_peer("n2", 5002),
                make_peer("n3", 5003),
            ],
        );

        // Grant vote to n3 in term 1
        node.handle_vote_request(VoteRequest {
            term: 1,
            candidate_id: "n3".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        });

        // Should reject n2 in same term
        let resp = node.handle_vote_request(VoteRequest {
            term: 1,
            candidate_id: "n2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(!resp.vote_granted);
    }

    #[test]
    fn test_vote_request_allows_higher_term_shorter_log() {
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let me = make_peer("n1", 5001);
        let config = ClusterConfiguration::stable(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
        ]);
        log_store.append(vec![
            LogEntry::new(1, "n1".to_string(), b"a".to_vec()),
            LogEntry::new(1, "n1".to_string(), b"b".to_vec()),
            LogEntry::new(1, "n1".to_string(), b"c".to_vec()),
        ]);
        let mut node = RaftNode::new(me, 500, log_store, state_store, None, config, 100, 1024);

        let resp = node.handle_vote_request(VoteRequest {
            term: 2,
            candidate_id: "n2".to_string(),
            last_log_index: 1,
            last_log_term: 2,
        });

        assert!(resp.vote_granted);
    }

    #[test]
    fn test_append_entries_follower_accepts_entries() {
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let me = make_peer("n1", 5001);
        let config = ClusterConfiguration::stable(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
        ]);

        let mut node = RaftNode::new(
            me,
            500,
            log_store.clone(),
            state_store,
            None,
            config,
            100,
            1024,
        );

        let resp = node.handle_append_entries_request(AppendEntriesRequest {
            term: 1,
            leader_id: "n2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![
                LogEntry::new(1, "n2".to_string(), b"cmd1".to_vec()),
                LogEntry::new(1, "n2".to_string(), b"cmd2".to_vec()),
            ],
        });

        assert!(resp.success);
        assert_eq!(log_store.last_index(), 2);
        assert_eq!(log_store.term_at(1), 1);
        assert_eq!(log_store.entry_at(1).unwrap().data, b"cmd1");
    }

    #[test]
    fn test_append_entries_rejected_on_log_mismatch() {
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let me = make_peer("n1", 5001);
        let config = ClusterConfiguration::stable(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
        ]);

        let mut node = RaftNode::new(
            me,
            500,
            log_store.clone(),
            state_store,
            None,
            config,
            100,
            1024,
        );

        // Append some entries first with term 1
        log_store.append(vec![LogEntry::new(1, "n1".to_string(), b"old".to_vec())]);

        let resp = node.handle_append_entries_request(AppendEntriesRequest {
            term: 2,
            leader_id: "n2".to_string(),
            prev_log_index: 1,
            prev_log_term: 999, // Wrong term
            leader_commit: 0,
            entries: vec![],
        });

        assert!(!resp.success);
        assert_eq!(resp.match_index, 1); // Reports last_index
    }

    #[test]
    fn test_membership_majority() {
        let config = ClusterConfiguration::stable(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
        ]);

        assert_eq!(config.current_majority_size(), 2);

        let one: HashSet<String> = ["n1".to_string()].into();
        let two: HashSet<String> = ["n1".to_string(), "n2".to_string()].into();
        let three: HashSet<String> = ["n1".to_string(), "n2".to_string(), "n3".to_string()].into();

        assert!(!config.has_current_majority(&one));
        assert!(config.has_current_majority(&two));
        assert!(config.has_current_majority(&three));
    }

    #[test]
    fn test_membership_joint_consensus() {
        let old = ClusterConfiguration::stable(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
        ]);

        let joint = old.transition_to(vec![
            make_peer("n1", 5001),
            make_peer("n2", 5002),
            make_peer("n3", 5003),
            make_peer("n4", 5004),
            make_peer("n5", 5005),
        ]);

        assert!(joint.is_joint_consensus());
        assert_eq!(joint.current_majority_size(), 2); // 3 -> maj=2
        assert_eq!(joint.next_majority_size(), 3); // 5 -> maj=3

        // n1,n2 gives old majority (2/3) but not new majority (2/5)
        let partial: HashSet<String> = ["n1".to_string(), "n2".to_string()].into();
        assert!(joint.has_current_majority(&partial));
        assert!(!joint.has_next_majority(&partial));
        assert!(!joint.has_joint_majority(&partial));

        // All five is majority of both
        let all: HashSet<String> = ["n1", "n2", "n3", "n4", "n5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(joint.has_joint_majority(&all));
    }

    #[test]
    fn test_snapshot_install_and_compact() {
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let me = make_peer("n1", 5001);
        let config = ClusterConfiguration::stable(vec![make_peer("n1", 5001)]);

        let mut node = RaftNode::new(
            me,
            500,
            log_store.clone(),
            state_store,
            None,
            config,
            100,
            1024,
        );

        // Install a snapshot
        node.accept_snapshot_chunk(InstallSnapshotRequest {
            term: 1,
            leader_id: "n1".to_string(),
            last_included_index: 10,
            last_included_term: 1,
            offset: 0,
            data: b"snapshot_data".to_vec(),
            done: true,
        });

        assert_eq!(log_store.snapshot_index(), 10);
        assert_eq!(log_store.snapshot_term(), 1);
        assert_eq!(log_store.snapshot_data(), b"snapshot_data");
        assert_eq!(log_store.last_index(), 10);
    }

    #[test]
    fn test_log_append_and_truncate() {
        let log_store = InMemoryLogStore::new();

        assert_eq!(log_store.last_index(), 0);
        assert_eq!(log_store.last_term(), 0);

        log_store.append(vec![
            LogEntry::new(1, "n1".to_string(), b"a".to_vec()),
            LogEntry::new(1, "n1".to_string(), b"b".to_vec()),
            LogEntry::new(2, "n1".to_string(), b"c".to_vec()),
        ]);

        assert_eq!(log_store.last_index(), 3);
        assert_eq!(log_store.last_term(), 2);
        assert_eq!(log_store.term_at(1), 1);
        assert_eq!(log_store.term_at(2), 1);
        assert_eq!(log_store.term_at(3), 2);

        log_store.truncate_from(2);

        assert_eq!(log_store.last_index(), 1);
        assert_eq!(log_store.entry_at(2), None);
    }

    #[test]
    fn test_non_voter_not_in_active_config() {
        let me = make_peer("n1", 5001);
        let config =
            ClusterConfiguration::stable(vec![make_peer("n2", 5002), make_peer("n3", 5003)]);
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());

        let node = RaftNode::new(me, 500, log_store, state_store, None, config, 100, 1024);

        assert!(!node.is_peer_in_active_config());
        assert!(!node.is_voter_in_active_config());
    }

    #[test]
    fn test_non_voter_timeout_marks_decommissioned() {
        let me = make_peer("n1", 5001);
        let config =
            ClusterConfiguration::stable(vec![make_peer("n2", 5002), make_peer("n3", 5003)]);
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let mut node = RaftNode::new(me, 500, log_store, state_store, None, config, 100, 1024);

        node.check_timeout(std::time::Instant::now(), &NoopTransport);

        assert!(node.is_decommissioned());
        assert_eq!(node.state, graft_core::types::NodeState::Follower);
    }

    #[test]
    fn test_snapshot_install_restores_state_machine_and_advances_apply_index() {
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());
        let sm = Arc::new(RestoreTrackingStateMachine::new());
        let me = make_peer("n1", 5001);
        let config = ClusterConfiguration::stable(vec![make_peer("n1", 5001)]);
        let mut node = RaftNode::new(
            me,
            500,
            log_store,
            state_store,
            Some(sm.clone()),
            config,
            100,
            1024,
        );

        node.accept_snapshot_chunk(InstallSnapshotRequest {
            term: 1,
            leader_id: "n2".to_string(),
            last_included_index: 7,
            last_included_term: 1,
            offset: 0,
            data: b"domain-snapshot".to_vec(),
            done: true,
        });

        assert_eq!(sm.restored(), b"domain-snapshot");
        assert_eq!(node.commit_index, 7);
        assert_eq!(node.last_applied, 7);
    }

    #[test]
    fn test_file_log_store_persistence() {
        use graft_storage::log_store::FileLogStore;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("raft.log");

        {
            let log = FileLogStore::new(path.clone());
            assert_eq!(log.last_index(), 0);
            log.append(vec![
                LogEntry::new(1, "n1".to_string(), b"hello".to_vec()),
                LogEntry::new(1, "n1".to_string(), b"world".to_vec()),
            ]);
            assert_eq!(log.last_index(), 2);
            log.install_snapshot(2, 1, b"snap".to_vec());
            assert_eq!(log.snapshot_index(), 2);
        }

        // Re-open and verify
        let log2 = FileLogStore::new(path);
        assert_eq!(log2.snapshot_index(), 2);
        assert_eq!(log2.snapshot_term(), 1);
        assert_eq!(log2.snapshot_data(), b"snap");
    }

    #[test]
    fn test_membership_join_and_joint() {
        use graft_proto::raft;
        use graft_runtime::membership;

        let n1 = make_peer("n1", 5001);
        let _n2 = make_peer("n2", 5002);
        let config = ClusterConfiguration::stable(vec![n1.clone()]);
        let log_store = Arc::new(InMemoryLogStore::new());
        let state_store = Arc::new(InMemoryPersistentStateStore::new());

        let mut node = RaftNode::new(
            make_peer("n1", 5001),
            500,
            log_store.clone(),
            state_store,
            None,
            config,
            100,
            1024,
        );

        // JOIN n2 as learner
        let join_cmd = membership::encode_join_command(&raft::PeerSpec {
            id: "n2".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5002,
            role: "LEARNER".to_string(),
        });
        log_store.append(vec![LogEntry::new(1, "n1".to_string(), join_cmd)]);
        node.apply_committed_entries(); // This won't work directly, need commit_index advance

        // Actually, let's test via the proper path: submit command -> apply
        node.state = graft_core::types::NodeState::Leader;
        node.current_term = 1;
        node.submit_command(membership::encode_join_command(&raft::PeerSpec {
            id: "n2".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5002,
            role: "VOTER".to_string(),
        }))
        .unwrap();
        node.commit_index = node.log_store.last_index();
        node.apply_committed_entries();

        assert!(node.known_peers.contains_key("n2"));
        assert!(node.pending_join_ids.contains("n2"));

        // JOINT
        node.submit_command(membership::encode_joint_command(&[
            raft::PeerSpec {
                id: "n1".to_string(),
                host: "127.0.0.1".to_string(),
                port: 5001,
                role: "VOTER".to_string(),
            },
            raft::PeerSpec {
                id: "n2".to_string(),
                host: "127.0.0.1".to_string(),
                port: 5002,
                role: "VOTER".to_string(),
            },
        ]))
        .unwrap();
        node.commit_index = node.log_store.last_index();
        node.apply_committed_entries();

        assert!(node.cluster_configuration.is_joint_consensus());
        assert!(node.cluster_configuration.contains("n2"));
        assert!(node.pending_join_ids.is_empty());

        // FINALIZE
        node.submit_command(membership::encode_finalize_command())
            .unwrap();
        node.commit_index = node.log_store.last_index();
        node.apply_committed_entries();

        assert!(!node.cluster_configuration.is_joint_consensus());
        assert_eq!(node.cluster_configuration.current_members().len(), 2);
    }
}
