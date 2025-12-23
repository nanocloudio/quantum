//! CLI argument parsing tests for subscribe/publish commands.
//!
//! # Manual Verification Instructions (kcat parity)
//!
//! These tests verify programmatic CLI parsing. For full kcat parity, manually
//! verify the following scenarios against a running broker:
//!
//! ## Subscribe command
//! ```shell
//! # Basic subscription (should stream JSON to stdout)
//! quantum subscribe --cert client.pem --key client-key.pem --ca ca.pem \
//!   --topic "test/topic"
//!
//! # Multi-topic subscription
//! quantum subscribe --cert ... --topic "topic1,topic2,topic3"
//!
//! # QoS 0 subscription
//! quantum subscribe --cert ... --topic "test/#" --qos qos0
//!
//! # Raw output format
//! quantum subscribe --cert ... --topic "test/topic" --format raw
//! ```
//!
//! ## Publish command - single topic mode
//! ```shell
//! # Inline message
//! quantum publish --cert ... --topic "test/topic" --message "hello"
//!
//! # Stdin payload (type message, press Ctrl+D)
//! quantum publish --cert ... --topic "test/topic"
//!
//! # With retain flag
//! quantum publish --cert ... --topic "test/retained" --message "value" --retain
//! ```
//!
//! ## Publish command - multi-topic stdin mode
//! ```shell
//! # Default colon delimiter
//! echo "topic1:payload1" | quantum publish --cert ...
//!
//! # Tab delimiter (for topics with colons like "urn:device:123")
//! printf 'urn:device:123\tpayload' | quantum publish --cert ... --delimiter '\t'
//!
//! # Hex delimiter (unit separator 0x1f)
//! printf 'topic\x1fpayload' | quantum publish --cert ... --delimiter '0x1f'
//!
//! # Base64 binary payloads
//! echo "binary/topic:AQIDBA==" | quantum publish --cert ... --binary
//! ```
//!
//! ## TLS failure cases
//! ```shell
//! # Missing cert should fail with clear error
//! quantum subscribe --key key.pem --ca ca.pem --topic "test"
//!
//! # Wrong CA should fail TLS handshake
//! quantum subscribe --cert client.pem --key client-key.pem --ca wrong-ca.pem \
//!   --topic "test"
//! ```

use clap::Parser;
use quantum::cli::{Cli, Commands, OutputFormat, QosLevel};

/// Helper to parse CLI args, returning the Commands enum.
fn parse_args(args: &[&str]) -> Result<Commands, clap::Error> {
    let mut full_args = vec!["quantum"];
    full_args.extend(args);
    Cli::try_parse_from(full_args).map(|cli| cli.command)
}

/// Helper to get error string from failed parse.
fn parse_error(args: &[&str]) -> String {
    let mut full_args = vec!["quantum"];
    full_args.extend(args);
    match Cli::try_parse_from(full_args) {
        Ok(_) => panic!("expected parse error"),
        Err(e) => e.to_string(),
    }
}

// =============================================================================
// Delimiter parsing tests
// =============================================================================

#[test]
fn publish_delimiter_default_is_colon() {
    let cmd = parse_args(&[
        "publish", "--cert", "c.pem", "--key", "k.pem", "--ca", "ca.pem",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, ":");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_tab_escape() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "\\t",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "\t");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_newline_escape() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "\\n",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "\n");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_carriage_return_escape() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "\\r",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "\r");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_hex_lowercase() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "0x1f",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "\x1f");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_hex_uppercase() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "0X1F",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "\x1f");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_hex_pipe() {
    // 0x7c = '|'
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "0x7c",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "|");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_literal_pipe() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "|",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.delimiter, "|");
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_delimiter_invalid_hex_rejected() {
    let err = parse_error(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "0xgg",
    ]);

    assert!(err.contains("invalid hex delimiter"), "error was: {err}");
}

#[test]
fn publish_delimiter_non_utf8_hex_rejected() {
    // 0xff is not valid UTF-8 as a standalone byte
    let err = parse_error(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--delimiter",
        "0xff",
    ]);

    assert!(err.contains("not valid UTF-8"), "error was: {err}");
}

// =============================================================================
// Subscribe command tests
// =============================================================================

#[test]
fn subscribe_requires_topic() {
    let err = parse_error(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
    ]);

    assert!(
        err.contains("--topic"),
        "error should mention --topic: {err}"
    );
}

#[test]
fn subscribe_requires_cert() {
    let err = parse_error(&[
        "subscribe",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
    ]);

    assert!(err.contains("--cert"), "error should mention --cert: {err}");
}

#[test]
fn subscribe_requires_key() {
    let err = parse_error(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
    ]);

    assert!(err.contains("--key"), "error should mention --key: {err}");
}

#[test]
fn subscribe_requires_ca() {
    let err = parse_error(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--topic",
        "test",
    ]);

    assert!(err.contains("--ca"), "error should mention --ca: {err}");
}

#[test]
fn subscribe_parses_single_topic() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "sensors/temperature",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert_eq!(args.topic, vec!["sensors/temperature"]);
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_parses_comma_separated_topics() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "topic1,topic2,topic3",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert_eq!(args.topic, vec!["topic1", "topic2", "topic3"]);
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_default_qos_is_qos1() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert!(matches!(args.qos, QosLevel::Qos1));
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_qos_levels() {
    for (qos_arg, expected) in [
        ("qos0", QosLevel::Qos0),
        ("qos1", QosLevel::Qos1),
        ("qos2", QosLevel::Qos2),
    ] {
        let cmd = parse_args(&[
            "subscribe",
            "--cert",
            "c.pem",
            "--key",
            "k.pem",
            "--ca",
            "ca.pem",
            "--topic",
            "test",
            "--qos",
            qos_arg,
        ])
        .unwrap();

        if let Commands::Subscribe(args) = cmd {
            assert!(
                std::mem::discriminant(&args.qos) == std::mem::discriminant(&expected),
                "qos mismatch for {qos_arg}"
            );
        } else {
            panic!("expected Subscribe command");
        }
    }
}

#[test]
fn subscribe_default_format_is_json() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert!(matches!(args.format, OutputFormat::Json));
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_format_raw() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
        "--format",
        "raw",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert!(matches!(args.format, OutputFormat::Raw));
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_default_host_and_port() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert_eq!(args.tls.host, "127.0.0.1");
        assert_eq!(args.tls.port, 8883);
    } else {
        panic!("expected Subscribe command");
    }
}

#[test]
fn subscribe_custom_host_and_port() {
    let cmd = parse_args(&[
        "subscribe",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
        "--host",
        "broker.example.com",
        "--port",
        "8884",
    ])
    .unwrap();

    if let Commands::Subscribe(args) = cmd {
        assert_eq!(args.tls.host, "broker.example.com");
        assert_eq!(args.tls.port, 8884);
    } else {
        panic!("expected Subscribe command");
    }
}

// =============================================================================
// Publish command tests
// =============================================================================

#[test]
fn publish_requires_tls_flags() {
    // Missing all TLS flags
    let result = parse_args(&["publish"]);
    assert!(result.is_err());

    // Missing key and ca
    let result = parse_args(&["publish", "--cert", "c.pem"]);
    assert!(result.is_err());
}

#[test]
fn publish_message_requires_topic() {
    let err = parse_error(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--message",
        "hello",
    ]);

    // clap reports this as --message requiring --topic
    assert!(
        err.contains("--topic") || err.contains("--message"),
        "error was: {err}"
    );
}

#[test]
fn publish_single_topic_mode() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test/topic",
        "--message",
        "hello world",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.topic, Some("test/topic".to_string()));
        assert_eq!(args.message, Some("hello world".to_string()));
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_multi_topic_mode_no_topic() {
    let cmd = parse_args(&[
        "publish", "--cert", "c.pem", "--key", "k.pem", "--ca", "ca.pem",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert!(args.topic.is_none());
        assert!(args.message.is_none());
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_retain_flag() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--topic",
        "test",
        "--message",
        "value",
        "--retain",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert!(args.retain);
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_binary_flag() {
    let cmd = parse_args(&[
        "publish", "--cert", "c.pem", "--key", "k.pem", "--ca", "ca.pem", "--binary",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert!(args.binary);
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_default_qos_is_qos1() {
    let cmd = parse_args(&[
        "publish", "--cert", "c.pem", "--key", "k.pem", "--ca", "ca.pem",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert!(matches!(args.qos, QosLevel::Qos1));
    } else {
        panic!("expected Publish command");
    }
}

#[test]
fn publish_qos_levels() {
    for qos_arg in ["qos0", "qos1", "qos2"] {
        let cmd = parse_args(&[
            "publish", "--cert", "c.pem", "--key", "k.pem", "--ca", "ca.pem", "--qos", qos_arg,
        ])
        .unwrap();

        if let Commands::Publish(args) = cmd {
            let expected = match qos_arg {
                "qos0" => QosLevel::Qos0,
                "qos1" => QosLevel::Qos1,
                "qos2" => QosLevel::Qos2,
                _ => unreachable!(),
            };
            assert!(
                std::mem::discriminant(&args.qos) == std::mem::discriminant(&expected),
                "qos mismatch for {qos_arg}"
            );
        } else {
            panic!("expected Publish command");
        }
    }
}

#[test]
fn publish_custom_client_id() {
    let cmd = parse_args(&[
        "publish",
        "--cert",
        "c.pem",
        "--key",
        "k.pem",
        "--ca",
        "ca.pem",
        "--client-id",
        "my-publisher-001",
    ])
    .unwrap();

    if let Commands::Publish(args) = cmd {
        assert_eq!(args.tls.client_id, Some("my-publisher-001".to_string()));
    } else {
        panic!("expected Publish command");
    }
}
