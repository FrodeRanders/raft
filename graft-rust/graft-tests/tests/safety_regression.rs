#[cfg(test)]
mod tests {
    use graft_core::{
        membership::ClusterConfiguration,
        raft_node::*,
        types::{LogEntry, NodeState, Peer},
    };
    use graft_storage::log_store::{InMemoryLogStore, InMemoryPersistentStateStore};
    use std::sync::Arc;

    fn rust_node() -> RaftNode {
        let peers =
            ["a", "b", "c"].map(|id| Peer::voter(id.into(), "127.0.0.1:1234".parse().unwrap()));
        RaftNode::new(
            peers[0].clone(),
            500,
            Arc::new(InMemoryLogStore::new()),
            Arc::new(InMemoryPersistentStateStore::new()),
            None,
            ClusterConfiguration::stable(peers.to_vec()),
            100,
            1024,
        )
    }
    fn append(term: u64, entries: Vec<LogEntry>, commit: u64) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: "b".into(),
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: commit,
            entries,
        }
    }
    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term,
            peer_id: "b".into(),
            data: vec![],
        }
    }

    #[test]
    fn rust_retry_must_preserve_committed_suffix_and_commit_index() {
        let mut n = rust_node();
        assert!(
            n.handle_append_entries_request(append(1, vec![entry(1), entry(1)], 2))
                .success
        );
        assert_eq!(n.commit_index, 2);
        n.handle_append_entries_request(append(1, vec![entry(1)], 2));
        assert_eq!(
            (n.log_store.last_index(), n.commit_index, n.last_applied),
            (2, 2, 2)
        );
    }

    #[test]
    fn rust_old_term_response_must_not_commit_replacement() {
        let mut n = rust_node();
        n.current_term = 3;
        n.state = NodeState::Leader;
        n.log_store.append(vec![entry(3)]);
        n.handle_append_entries_response(
            AppendEntriesResponse {
                term: 1,
                peer_id: "b".into(),
                success: true,
                match_index: 1,
            },
            append(1, vec![entry(1)], 0),
            "b",
        );
        assert_eq!(n.commit_index, 0);
    }

    #[test]
    fn rust_same_term_stepdown_must_retain_self_vote() {
        let mut n = rust_node();
        n.current_term = 1;
        n.state = NodeState::Candidate;
        n.voted_for = Some("a".into());
        n.handle_append_entries_request(append(1, vec![], 0));
        let r = n.handle_vote_request(VoteRequest {
            term: 1,
            candidate_id: "c".into(),
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(!r.vote_granted);
    }

    #[test]
    fn rust_snapshot_retransmission_must_not_duplicate_bytes() {
        let mut n = rust_node();
        let req = |offset, data: &[u8], done| InstallSnapshotRequest {
            term: 1,
            leader_id: "b".into(),
            last_included_index: 2,
            last_included_term: 1,
            offset,
            data: data.to_vec(),
            done,
        };
        n.accept_snapshot_chunk(req(0, b"ab", false));
        n.accept_snapshot_chunk(req(2, b"cd", false));
        assert!(!n.accept_snapshot_chunk(req(2, b"cd", true)).success);
        assert!(n.accept_snapshot_chunk(req(4, b"", true)).success);
        assert_eq!(n.log_store.snapshot_data(), b"abcd");
    }

    #[test]
    fn rust_failed_storage_stops_the_process() {
        use graft_core::raft_node::PersistentStateStore;
        if let Ok(folder) = std::env::var("GRAFT_TEST_FAILED_STORAGE") {
            let folder = std::path::PathBuf::from(folder);
            std::fs::create_dir(&folder).unwrap();
            let store =
                graft_storage::state_store::FilePersistentStateStore::new(folder.join("state"));
            std::fs::remove_dir(&folder).unwrap();
            std::fs::write(&folder, b"blocks durable write").unwrap();
            store.set_current_term(7);
            panic!("persistence failure returned to caller");
        }
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::rust_failed_storage_stops_the_process",
                "--nocapture",
            ])
            .env(
                "GRAFT_TEST_FAILED_STORAGE",
                directory.path().join("blocked"),
            )
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("Raft persistence failed"), "{error}");
        assert!(!error.contains("returned to caller"));
    }

    #[test]
    fn rust_snapshot_must_restore_membership() {
        let mut n = rust_node();
        let bytes =
            graft_core::snapshot_codec::wrap_snapshot_payload(&["b".into(), "c".into()], &[], b"");
        n.accept_snapshot_chunk(InstallSnapshotRequest {
            term: 1,
            leader_id: "b".into(),
            last_included_index: 2,
            last_included_term: 1,
            offset: 0,
            data: bytes,
            done: true,
        });
        assert!(
            !n.cluster_configuration.contains("a"),
            "removed voter retained after membership snapshot install"
        );
    }

    #[test]
    fn durable_snapshot_preserves_matching_suffix_and_restart_identity() {
        use graft_core::raft_node::PersistentStateStore;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log");
        let store = graft_storage::log_store::FileLogStore::new(path.clone());
        store.append(vec![entry(1), entry(1), entry(2)]);
        store.install_snapshot(2, 1, b"snapshot".to_vec());
        assert_eq!(store.last_index(), 3);
        drop(store);
        let recovered = graft_storage::log_store::FileLogStore::new(path.clone());
        assert_eq!(recovered.snapshot_data(), b"snapshot");
        assert_eq!(recovered.entry_at(3).unwrap().peer_id, "b");
        recovered.truncate_from(3);
        assert_eq!(
            graft_storage::log_store::FileLogStore::new(path).last_index(),
            2
        );
        let state_path = directory.path().join("state");
        let state = graft_storage::state_store::FilePersistentStateStore::new(state_path.clone());
        state.set_current_term(5);
        state.set_voted_for(Some("b".into()));
        let state = graft_storage::state_store::FilePersistentStateStore::new(state_path);
        assert_eq!(state.current_term(), 5);
        assert_eq!(state.voted_for().as_deref(), Some("b"));
    }
}
