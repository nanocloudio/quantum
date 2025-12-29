//! Kafka protocol error codes and error handling.
//!
//! This module defines all Kafka error codes as specified in the Kafka protocol
//! documentation. Error codes are used in responses to indicate success or
//! various failure conditions.

/// Kafka error codes.
///
/// These match the official Kafka protocol error codes.
/// See: https://kafka.apache.org/protocol#protocol_error_codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ErrorCode {
    /// The server experienced an unexpected error when processing the request.
    UnknownServerError = -1,
    /// No error - success.
    None = 0,
    /// The requested offset is not within the range of offsets maintained by the server.
    OffsetOutOfRange = 1,
    /// This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
    CorruptMessage = 2,
    /// This server does not host this topic-partition.
    UnknownTopicOrPartition = 3,
    /// The requested fetch size is invalid.
    InvalidFetchSize = 4,
    /// There is no leader for this topic-partition as we are in the middle of a leadership election.
    LeaderNotAvailable = 5,
    /// For requests intended only for the leader, this error indicates that the broker is not the current leader.
    NotLeaderOrFollower = 6,
    /// The request timed out.
    RequestTimedOut = 7,
    /// The broker is not available.
    BrokerNotAvailable = 8,
    /// The replica is not available for the requested topic-partition.
    ReplicaNotAvailable = 9,
    /// The request included a message larger than the max message size the server will accept.
    MessageTooLarge = 10,
    /// The controller moved to another broker.
    StaleControllerEpoch = 11,
    /// The metadata field of the offset request was too large.
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The coordinator is loading and hence can't process requests.
    CoordinatorLoadInProgress = 14,
    /// The coordinator is not available.
    CoordinatorNotAvailable = 15,
    /// This is not the correct coordinator.
    NotCoordinator = 16,
    /// The request attempted to perform an operation on an invalid topic.
    InvalidTopicException = 17,
    /// The request included message batch larger than the configured segment size on the server.
    RecordListTooLarge = 18,
    /// Messages are rejected since there are fewer in-sync replicas than required.
    NotEnoughReplicas = 19,
    /// Messages are written to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Produce request specified an invalid value for required acks.
    InvalidRequiredAcks = 21,
    /// Specified group generation id is not valid.
    IllegalGeneration = 22,
    /// The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
    InconsistentGroupProtocol = 23,
    /// The configured groupId is invalid.
    InvalidGroupId = 24,
    /// The coordinator is not aware of this member.
    UnknownMemberId = 25,
    /// The session timeout is not within the range allowed by the broker.
    InvalidSessionTimeout = 26,
    /// The group is rebalancing, so a rejoin is needed.
    RebalanceInProgress = 27,
    /// The committing offset data size is not valid.
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
    /// Topic with this name already exists.
    TopicAlreadyExists = 36,
    /// Number of partitions is below 1.
    InvalidPartitions = 37,
    /// Replication factor is below 1 or larger than the number of available brokers.
    InvalidReplicationFactor = 38,
    /// Replica assignment is invalid.
    InvalidReplicaAssignment = 39,
    /// Configuration is invalid.
    InvalidConfig = 40,
    /// This is not the correct controller for this cluster.
    NotController = 41,
    /// This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker.
    InvalidRequest = 42,
    /// The message format version on the broker does not support the request.
    UnsupportedForMessageFormat = 43,
    /// Request parameters do not satisfy the configured policy.
    PolicyViolation = 44,
    /// The broker received an out of order sequence number.
    OutOfOrderSequenceNumber = 45,
    /// The broker received a duplicate sequence number.
    DuplicateSequenceNumber = 46,
    /// Producer attempted to produce with an old epoch.
    InvalidProducerEpoch = 47,
    /// The producer attempted a transactional operation in an invalid state.
    InvalidTxnState = 48,
    /// The producer attempted to use a producer id which is not currently assigned to its transactional id.
    InvalidProducerIdMapping = 49,
    /// The transaction timeout is larger than the maximum value allowed by the broker.
    InvalidTransactionTimeout = 50,
    /// The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled.
    SecurityDisabled = 54,
    /// The broker did not attempt to execute this operation.
    OperationNotAttempted = 55,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
    /// This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question.
    UnknownProducerId = 59,
    /// A partition reassignment is in progress.
    ReassignmentInProgress = 60,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation Token is expired.
    DelegationTokenExpired = 66,
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67,
    /// The group is not empty.
    NonEmptyGroup = 68,
    /// The group id does not exist.
    GroupIdNotFound = 69,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71,
    /// There is no listener on the leader broker that matches the listener on which metadata request was processed.
    ListenerNotFound = 72,
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73,
    /// The leader epoch in the request is older than the epoch on the broker.
    FencedLeaderEpoch = 74,
    /// The leader epoch in the request is newer than the epoch on the broker.
    UnknownLeaderEpoch = 75,
    /// The requesting client does not support the compression type of given partition.
    UnsupportedCompressionType = 76,
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77,
    /// The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.
    OffsetNotAvailable = 78,
    /// The group member needs to have a valid member id before actually entering a consumer group.
    MemberIdRequired = 79,
    /// The preferred leader was not available.
    PreferredLeaderNotAvailable = 80,
    /// The consumer group has reached its max size.
    GroupMaxSizeReached = 81,
    /// The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.
    FencedInstanceId = 82,
    /// Eligible topic partition leaders are not available.
    EligibleLeadersNotAvailable = 83,
    /// Leader election not needed for topic partition.
    ElectionNotNeeded = 84,
    /// No partition reassignment is in progress.
    NoReassignmentInProgress = 85,
    /// Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
    GroupSubscribedToTopic = 86,
    /// This record has failed the validation on broker and hence will be rejected.
    InvalidRecord = 87,
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit = 88,
    /// The throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89,
    /// There is a newer producer with the same transactionalId which fences the current one.
    ProducerFenced = 90,
    /// A request illegally referred to a resource that does not exist.
    ResourceNotFound = 91,
    /// A request illegally referred to the same resource twice.
    DuplicateResource = 92,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93,
    /// Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.
    InconsistentVoterSet = 94,
    /// The given update version was invalid.
    InvalidUpdateVersion = 95,
    /// Unable to update finalized features due to an unexpected server error.
    FeatureUpdateFailed = 96,
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97,
    /// Unknown topic id.
    UnknownTopicId = 100,
    /// The member epoch is fenced by the group coordinator.
    FencedMemberEpoch = 110,
    /// The instance ID is still used by another member in the consumer group.
    UnreleasedInstanceId = 111,
    /// The assignor or its version range is not supported by the consumer group.
    UnsupportedAssignor = 112,
}

impl ErrorCode {
    /// Parse error code from i16.
    pub fn from_i16(value: i16) -> Self {
        match value {
            -1 => Self::UnknownServerError,
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidFetchSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            11 => Self::StaleControllerEpoch,
            12 => Self::OffsetMetadataTooLarge,
            13 => Self::NetworkException,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicException,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            24 => Self::InvalidGroupId,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            32 => Self::InvalidTimestamp,
            33 => Self::UnsupportedSaslMechanism,
            34 => Self::IllegalSaslState,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            39 => Self::InvalidReplicaAssignment,
            40 => Self::InvalidConfig,
            41 => Self::NotController,
            42 => Self::InvalidRequest,
            43 => Self::UnsupportedForMessageFormat,
            44 => Self::PolicyViolation,
            45 => Self::OutOfOrderSequenceNumber,
            46 => Self::DuplicateSequenceNumber,
            47 => Self::InvalidProducerEpoch,
            48 => Self::InvalidTxnState,
            49 => Self::InvalidProducerIdMapping,
            50 => Self::InvalidTransactionTimeout,
            51 => Self::ConcurrentTransactions,
            52 => Self::TransactionCoordinatorFenced,
            53 => Self::TransactionalIdAuthorizationFailed,
            54 => Self::SecurityDisabled,
            55 => Self::OperationNotAttempted,
            56 => Self::KafkaStorageError,
            57 => Self::LogDirNotFound,
            58 => Self::SaslAuthenticationFailed,
            59 => Self::UnknownProducerId,
            60 => Self::ReassignmentInProgress,
            61 => Self::DelegationTokenAuthDisabled,
            62 => Self::DelegationTokenNotFound,
            63 => Self::DelegationTokenOwnerMismatch,
            64 => Self::DelegationTokenRequestNotAllowed,
            65 => Self::DelegationTokenAuthorizationFailed,
            66 => Self::DelegationTokenExpired,
            67 => Self::InvalidPrincipalType,
            68 => Self::NonEmptyGroup,
            69 => Self::GroupIdNotFound,
            70 => Self::FetchSessionIdNotFound,
            71 => Self::InvalidFetchSessionEpoch,
            72 => Self::ListenerNotFound,
            73 => Self::TopicDeletionDisabled,
            74 => Self::FencedLeaderEpoch,
            75 => Self::UnknownLeaderEpoch,
            76 => Self::UnsupportedCompressionType,
            77 => Self::StaleBrokerEpoch,
            78 => Self::OffsetNotAvailable,
            79 => Self::MemberIdRequired,
            80 => Self::PreferredLeaderNotAvailable,
            81 => Self::GroupMaxSizeReached,
            82 => Self::FencedInstanceId,
            83 => Self::EligibleLeadersNotAvailable,
            84 => Self::ElectionNotNeeded,
            85 => Self::NoReassignmentInProgress,
            86 => Self::GroupSubscribedToTopic,
            87 => Self::InvalidRecord,
            88 => Self::UnstableOffsetCommit,
            89 => Self::ThrottlingQuotaExceeded,
            90 => Self::ProducerFenced,
            91 => Self::ResourceNotFound,
            92 => Self::DuplicateResource,
            93 => Self::UnacceptableCredential,
            94 => Self::InconsistentVoterSet,
            95 => Self::InvalidUpdateVersion,
            96 => Self::FeatureUpdateFailed,
            97 => Self::PrincipalDeserializationFailure,
            100 => Self::UnknownTopicId,
            110 => Self::FencedMemberEpoch,
            111 => Self::UnreleasedInstanceId,
            112 => Self::UnsupportedAssignor,
            _ => Self::UnknownServerError,
        }
    }

    /// Get the error code as i16.
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    /// Check if this represents success (no error).
    pub fn is_success(self) -> bool {
        matches!(self, Self::None)
    }

    /// Check if this is a retriable error.
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            Self::CoordinatorLoadInProgress
                | Self::CoordinatorNotAvailable
                | Self::NotCoordinator
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::RequestTimedOut
                | Self::RebalanceInProgress
                | Self::UnknownMemberId
                | Self::NetworkException
                | Self::OffsetNotAvailable
                | Self::NotEnoughReplicas
                | Self::NotEnoughReplicasAfterAppend
        )
    }

    /// Get a human-readable error message.
    pub fn message(self) -> &'static str {
        match self {
            Self::UnknownServerError => "The server experienced an unexpected error",
            Self::None => "Success",
            Self::OffsetOutOfRange => "The requested offset is out of range",
            Self::CorruptMessage => "The message is corrupt",
            Self::UnknownTopicOrPartition => "Unknown topic or partition",
            Self::InvalidFetchSize => "Invalid fetch size",
            Self::LeaderNotAvailable => "Leader not available",
            Self::NotLeaderOrFollower => "Not leader or follower",
            Self::RequestTimedOut => "Request timed out",
            Self::BrokerNotAvailable => "Broker not available",
            Self::ReplicaNotAvailable => "Replica not available",
            Self::MessageTooLarge => "Message too large",
            Self::StaleControllerEpoch => "Stale controller epoch",
            Self::OffsetMetadataTooLarge => "Offset metadata too large",
            Self::NetworkException => "Network exception",
            Self::CoordinatorLoadInProgress => "Coordinator load in progress",
            Self::CoordinatorNotAvailable => "Coordinator not available",
            Self::NotCoordinator => "Not coordinator",
            Self::InvalidTopicException => "Invalid topic",
            Self::RecordListTooLarge => "Record list too large",
            Self::NotEnoughReplicas => "Not enough replicas",
            Self::NotEnoughReplicasAfterAppend => "Not enough replicas after append",
            Self::InvalidRequiredAcks => "Invalid required acks",
            Self::IllegalGeneration => "Illegal generation",
            Self::InconsistentGroupProtocol => "Inconsistent group protocol",
            Self::InvalidGroupId => "Invalid group ID",
            Self::UnknownMemberId => "Unknown member ID",
            Self::InvalidSessionTimeout => "Invalid session timeout",
            Self::RebalanceInProgress => "Rebalance in progress",
            Self::InvalidCommitOffsetSize => "Invalid commit offset size",
            Self::TopicAuthorizationFailed => "Topic authorization failed",
            Self::GroupAuthorizationFailed => "Group authorization failed",
            Self::ClusterAuthorizationFailed => "Cluster authorization failed",
            Self::InvalidTimestamp => "Invalid timestamp",
            Self::UnsupportedSaslMechanism => "Unsupported SASL mechanism",
            Self::IllegalSaslState => "Illegal SASL state",
            Self::UnsupportedVersion => "Unsupported version",
            Self::TopicAlreadyExists => "Topic already exists",
            Self::InvalidPartitions => "Invalid partitions",
            Self::InvalidReplicationFactor => "Invalid replication factor",
            Self::InvalidReplicaAssignment => "Invalid replica assignment",
            Self::InvalidConfig => "Invalid config",
            Self::NotController => "Not controller",
            Self::InvalidRequest => "Invalid request",
            Self::UnsupportedForMessageFormat => "Unsupported for message format",
            Self::PolicyViolation => "Policy violation",
            Self::OutOfOrderSequenceNumber => "Out of order sequence number",
            Self::DuplicateSequenceNumber => "Duplicate sequence number",
            Self::InvalidProducerEpoch => "Invalid producer epoch",
            Self::InvalidTxnState => "Invalid transaction state",
            Self::InvalidProducerIdMapping => "Invalid producer ID mapping",
            Self::InvalidTransactionTimeout => "Invalid transaction timeout",
            Self::ConcurrentTransactions => "Concurrent transactions",
            Self::TransactionCoordinatorFenced => "Transaction coordinator fenced",
            Self::TransactionalIdAuthorizationFailed => "Transactional ID authorization failed",
            Self::SecurityDisabled => "Security disabled",
            Self::OperationNotAttempted => "Operation not attempted",
            Self::KafkaStorageError => "Kafka storage error",
            Self::LogDirNotFound => "Log dir not found",
            Self::SaslAuthenticationFailed => "SASL authentication failed",
            Self::UnknownProducerId => "Unknown producer ID",
            Self::ReassignmentInProgress => "Reassignment in progress",
            Self::DelegationTokenAuthDisabled => "Delegation token auth disabled",
            Self::DelegationTokenNotFound => "Delegation token not found",
            Self::DelegationTokenOwnerMismatch => "Delegation token owner mismatch",
            Self::DelegationTokenRequestNotAllowed => "Delegation token request not allowed",
            Self::DelegationTokenAuthorizationFailed => "Delegation token authorization failed",
            Self::DelegationTokenExpired => "Delegation token expired",
            Self::InvalidPrincipalType => "Invalid principal type",
            Self::NonEmptyGroup => "Non-empty group",
            Self::GroupIdNotFound => "Group ID not found",
            Self::FetchSessionIdNotFound => "Fetch session ID not found",
            Self::InvalidFetchSessionEpoch => "Invalid fetch session epoch",
            Self::ListenerNotFound => "Listener not found",
            Self::TopicDeletionDisabled => "Topic deletion disabled",
            Self::FencedLeaderEpoch => "Fenced leader epoch",
            Self::UnknownLeaderEpoch => "Unknown leader epoch",
            Self::UnsupportedCompressionType => "Unsupported compression type",
            Self::StaleBrokerEpoch => "Stale broker epoch",
            Self::OffsetNotAvailable => "Offset not available",
            Self::MemberIdRequired => "Member ID required",
            Self::PreferredLeaderNotAvailable => "Preferred leader not available",
            Self::GroupMaxSizeReached => "Group max size reached",
            Self::FencedInstanceId => "Fenced instance ID",
            Self::EligibleLeadersNotAvailable => "Eligible leaders not available",
            Self::ElectionNotNeeded => "Election not needed",
            Self::NoReassignmentInProgress => "No reassignment in progress",
            Self::GroupSubscribedToTopic => "Group subscribed to topic",
            Self::InvalidRecord => "Invalid record",
            Self::UnstableOffsetCommit => "Unstable offset commit",
            Self::ThrottlingQuotaExceeded => "Throttling quota exceeded",
            Self::ProducerFenced => "Producer fenced",
            Self::ResourceNotFound => "Resource not found",
            Self::DuplicateResource => "Duplicate resource",
            Self::UnacceptableCredential => "Unacceptable credential",
            Self::InconsistentVoterSet => "Inconsistent voter set",
            Self::InvalidUpdateVersion => "Invalid update version",
            Self::FeatureUpdateFailed => "Feature update failed",
            Self::PrincipalDeserializationFailure => "Principal deserialization failure",
            Self::UnknownTopicId => "Unknown topic ID",
            Self::FencedMemberEpoch => "Fenced member epoch",
            Self::UnreleasedInstanceId => "Unreleased instance ID",
            Self::UnsupportedAssignor => "Unsupported assignor",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message(), self.as_i16())
    }
}

impl std::error::Error for ErrorCode {}

impl Default for ErrorCode {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_round_trip() {
        for code in [
            ErrorCode::None,
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::LeaderNotAvailable,
            ErrorCode::RebalanceInProgress,
            ErrorCode::UnknownMemberId,
        ] {
            let value = code.as_i16();
            let parsed = ErrorCode::from_i16(value);
            assert_eq!(parsed, code);
        }
    }

    #[test]
    fn test_error_code_retriable() {
        assert!(ErrorCode::LeaderNotAvailable.is_retriable());
        assert!(ErrorCode::RebalanceInProgress.is_retriable());
        assert!(!ErrorCode::None.is_retriable());
        assert!(!ErrorCode::InvalidRequest.is_retriable());
    }

    #[test]
    fn test_error_code_success() {
        assert!(ErrorCode::None.is_success());
        assert!(!ErrorCode::UnknownServerError.is_success());
    }
}
