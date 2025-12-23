use quantum::raft_net::{RaftNetConfig, RaftNetResources};
use std::path::PathBuf;

#[test]
fn raft_net_paths_are_reused() {
    let cfg = RaftNetConfig {
        local_id: "n0".into(),
        bind: Some("127.0.0.1:19001".into()),
        tls_chain_path: PathBuf::from("chain.pem"),
        tls_key_path: PathBuf::from("key.pem"),
        trust_bundle: PathBuf::from("ca.pem"),
        trust_domain: "local".into(),
        log_dir: PathBuf::from("data/raft"),
    };
    let net = RaftNetResources::new(cfg.clone());
    let paths = net.paths();
    assert_eq!(paths.tls_chain, cfg.tls_chain_path);
    assert_eq!(paths.tls_key, cfg.tls_key_path);
    assert_eq!(paths.trust_bundle, cfg.trust_bundle);
    assert_eq!(paths.trust_domain, cfg.trust_domain);
    assert_eq!(paths.log_root, cfg.log_dir);
    assert_eq!(paths.bind_override, cfg.bind);
}
