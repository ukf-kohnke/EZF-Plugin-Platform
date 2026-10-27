//! Versioned IPC protocol primitives for a future process-backed plugin executor.
//!
//! This crate intentionally contains no process spawning, sockets, OS sandboxing, plugin loading,
//! or host services. It defines a stable wire frame and bounded synchronous framing helpers. The
//! actual transport and request multiplexing will be layered on top in a later phase.
#![allow(non_snake_case)]
use serde::{ de::DeserializeOwned, Deserialize, Serialize };
use std::fmt;
use std::io::{ self, Read, Write };
use std::sync::{ Arc, Mutex };

pub const IPC_MAGIC: [u8; 4] = *b"EZFI";
pub const IPC_MAJOR_VERSION: u16 = 1;
pub const IPC_MINOR_VERSION: u16 = 0;

/// Header layout (little-endian): magic[4], major[2], minor[2], type[2], flags[2],
/// request_id[8], correlation_id[8], control_len[4], payload_len[4].
pub const IPC_HEADER_LEN: usize = 36;
pub const DEFAULT_MAX_CONTROL_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_FRAME_BYTES: usize =
    IPC_HEADER_LEN + DEFAULT_MAX_CONTROL_BYTES + DEFAULT_MAX_PAYLOAD_BYTES;

/// Flags are reserved in protocol v1. Receivers reject non-zero values rather than silently
/// assigning semantics to bits that may have been produced by an incompatible peer.
pub const IPC_FLAGS_V1: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u16)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Hello = 1,
    Initialize = 2,
    LoadPlugin = 3,
    Invoke = 4,
    Cancel = 5,
    CapabilityResult = 6,
    HealthProbe = 7,
    Shutdown = 8,
    SandboxReady = 9,
    Initialized = 10,
    Result = 11,
    CapabilityRequest = 12,
    Log = 13,
    Pong = 14,
    ShutdownComplete = 15,
    ProtocolError = 16,
}

impl TryFrom<u16> for MessageType {
    type Error = FrameError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::Initialize),
            3 => Ok(Self::LoadPlugin),
            4 => Ok(Self::Invoke),
            5 => Ok(Self::Cancel),
            6 => Ok(Self::CapabilityResult),
            7 => Ok(Self::HealthProbe),
            8 => Ok(Self::Shutdown),
            9 => Ok(Self::SandboxReady),
            10 => Ok(Self::Initialized),
            11 => Ok(Self::Result),
            12 => Ok(Self::CapabilityRequest),
            13 => Ok(Self::Log),
            14 => Ok(Self::Pong),
            15 => Ok(Self::ShutdownComplete),
            16 => Ok(Self::ProtocolError),
            other => Err(FrameError::UnknownMessageType(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvokeOperation {
    Name,
    Manifest,
    IsLoaded,
    HandleEndpoint,
    ExecuteDatabase,
    SessionClosed,
    OnUnload,
    Destroy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    DatabaseExecute,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxProfile {
    DomainPlugin,
    DataSourcePlugin,
    DevelopmentUnconfined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpcErrorCode {
    PluginError,
    Cancelled,
    WorkerExited,
    ProtocolViolation,
    SandboxSetupFailed,
    CapabilityDenied,
    ResourceLimitExceeded,
    UnsupportedProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeLimits {
    pub max_concurrent_calls: u32,
    pub max_payload_bytes: u32,
    pub cancellation_grace_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloControl {
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub worker_build: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeControl {
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub worker_build: String,
    pub plugin_path: String,
    pub plugin_directory: String,
    pub allowed_capabilities: Vec<CapabilityKind>,
    pub runtime_limits: RuntimeLimits,
    pub sandbox_profile: SandboxProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvokeControl {
    pub operation: InvokeOperation,
    /// Remaining time at send-time, in milliseconds. An absolute `Instant` is never transmitted.
    pub remaining_budget_ms: u64,
    pub operation_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadPluginControl {
    pub expected_abi_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelControl {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthProbeControl {
    pub probe_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxReadyControl {
    pub profile: SandboxProfile,
    pub sandbox_enforced: bool,
    pub active_features: Vec<String>,
    pub missing_features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PongControl {
    pub probe_id: u64,
    pub worker_uptime_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownControl {
    pub grace_budget_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownCompleteControl {
    pub graceful: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializedControl {
    pub plugin_name: String,
    /// Serialized manifest bytes are represented as JSON text to keep this crate independent of
    /// the host's PluginContracts crate. The worker must not be trusted as an identity authority.
    pub manifest_json: String,
    pub abi_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultControl {
    pub success: bool,
    pub error_code: Option<IpcErrorCode>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRequestControl {
    pub operation_id: u64,
    pub capability: CapabilityKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityResultControl {
    pub success: bool,
    pub error_code: Option<IpcErrorCode>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogControl {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolErrorControl {
    pub code: IpcErrorCode,
    pub message: String,
}

/// Resource limits are applied before allocations on receive and before writing on send.
/// `max_frame_bytes` includes the fixed header and both body sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLimits {
    pub max_control_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_frame_bytes: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_control_bytes: DEFAULT_MAX_CONTROL_BYTES,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

/// Shared byte budget for payloads retained by concurrent in-flight requests on one connection.
/// The future IPC multiplexer should hold a reservation until it has delivered or discarded the
/// associated payload, rather than only until the frame has been read from the transport.
#[derive(Debug, Clone)]
pub struct InFlightPayloadBudget {
    maximum_bytes: usize,
    used_bytes: Arc<Mutex<usize>>,
}

impl InFlightPayloadBudget {
    pub fn new(maximum_bytes: usize) -> Self {
        Self { maximum_bytes, used_bytes: Arc::new(Mutex::new(0)) }
    }

    pub fn used_bytes(&self) -> usize {
        *self.used_bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn reserve(&self, bytes: usize) -> Result<PayloadReservation, PayloadBudgetError> {
        let mut used = self.used_bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let attempted = (*used).checked_add(bytes).ok_or(PayloadBudgetError::SizeOverflow)?;
        if attempted > self.maximum_bytes {
            return Err(PayloadBudgetError::LimitExceeded {
                requested_bytes: bytes,
                used_bytes: *used,
                maximum_bytes: self.maximum_bytes,
            });
        }
        *used = attempted;
        Ok(PayloadReservation { used_bytes: Arc::clone(&self.used_bytes), reserved_bytes: bytes })
    }
}

/// RAII reservation; dropping it releases the corresponding in-flight payload bytes.
#[derive(Debug)]
pub struct PayloadReservation {
    used_bytes: Arc<Mutex<usize>>,
    reserved_bytes: usize,
}

impl PayloadReservation {
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
}

impl Drop for PayloadReservation {
    fn drop(&mut self) {
        let mut used = self.used_bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Saturating subtraction prevents a secondary panic during unwind; a zero value indicates
        // an internal accounting invariant bug and should be caught by the tests.
        *used = (*used).saturating_sub(self.reserved_bytes);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadBudgetError {
    LimitExceeded {
        requested_bytes: usize,
        used_bytes: usize,
        maximum_bytes: usize,
    },
    SizeOverflow,
}

impl fmt::Display for PayloadBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded { requested_bytes, used_bytes, maximum_bytes } =>
                write!(
                    f,
                    "in-flight IPC payload budget exceeded: {used_bytes} bytes used, {requested_bytes} requested, limit {maximum_bytes}"
                ),
            Self::SizeOverflow => f.write_str("in-flight IPC payload size overflow"),
        }
    }
}

impl std::error::Error for PayloadBudgetError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcFrame {
    pub message_type: MessageType,
    pub flags: u16,
    pub request_id: u64,
    /// Correlates capability/result traffic with the logical call that caused it.
    pub correlation_id: Option<u64>,
    /// Bounded JSON control data. Binary request/response data belongs in `payload`.
    pub control: Vec<u8>,
    /// Raw ABI request/result bytes. Never encoded as a JSON number array or base64 string.
    pub payload: Vec<u8>,
}

impl IpcFrame {
    pub fn new(message_type: MessageType, request_id: u64) -> Self {
        Self {
            message_type,
            flags: IPC_FLAGS_V1,
            request_id,
            correlation_id: None,
            control: b"{}".to_vec(),
            payload: Vec::new(),
        }
    }

    pub fn with_control<T: Serialize>(
        message_type: MessageType,
        request_id: u64,
        control: &T
    ) -> Result<Self, FrameError> {
        let mut frame = Self::new(message_type, request_id);
        frame.set_control(control)?;
        Ok(frame)
    }

    pub fn set_control<T: Serialize>(&mut self, control: &T) -> Result<(), FrameError> {
        self.control = serde_json::to_vec(control).map_err(FrameError::ControlEncode)?;
        Ok(())
    }

    pub fn decode_control<T: DeserializeOwned>(&self) -> Result<T, FrameError> {
        serde_json::from_slice(&self.control).map_err(FrameError::ControlDecode)
    }

    pub fn write_to<W: Write>(
        &self,
        writer: &mut W,
        limits: FrameLimits
    ) -> Result<(), FrameError> {
        validate_lengths(self.control.len(), self.payload.len(), limits)?;
        validate_flags_and_correlation(self.flags, self.correlation_id)?;
        validate_message_correlation(self.message_type, self.correlation_id)?;
        let control_len = u32
            ::try_from(self.control.len())
            .map_err(|_| FrameError::LengthOverflow)?;
        let payload_len = u32
            ::try_from(self.payload.len())
            .map_err(|_| FrameError::LengthOverflow)?;

        let mut header = [0u8; IPC_HEADER_LEN];
        header[0..4].copy_from_slice(&IPC_MAGIC);
        header[4..6].copy_from_slice(&IPC_MAJOR_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&IPC_MINOR_VERSION.to_le_bytes());
        header[8..10].copy_from_slice(&(self.message_type as u16).to_le_bytes());
        header[10..12].copy_from_slice(&self.flags.to_le_bytes());
        header[12..20].copy_from_slice(&self.request_id.to_le_bytes());
        header[20..28].copy_from_slice(&self.correlation_id.unwrap_or(0).to_le_bytes());
        header[28..32].copy_from_slice(&control_len.to_le_bytes());
        header[32..36].copy_from_slice(&payload_len.to_le_bytes());

        writer.write_all(&header).map_err(FrameError::Io)?;
        writer.write_all(&self.control).map_err(FrameError::Io)?;
        writer.write_all(&self.payload).map_err(FrameError::Io)?;
        Ok(())
    }

    pub fn read_from<R: Read>(reader: &mut R, limits: FrameLimits) -> Result<Self, FrameError> {
        let mut header = [0u8; IPC_HEADER_LEN];
        reader.read_exact(&mut header).map_err(FrameError::Io)?;

        if &header[0..4] != &IPC_MAGIC[..] {
            return Err(FrameError::InvalidMagic);
        }
        let major = read_u16(&header[4..6]);
        let minor = read_u16(&header[6..8]);
        if major != IPC_MAJOR_VERSION || minor != IPC_MINOR_VERSION {
            return Err(FrameError::UnsupportedVersion { major, minor });
        }

        let message_type = MessageType::try_from(read_u16(&header[8..10]))?;
        let flags = read_u16(&header[10..12]);
        let request_id = read_u64(&header[12..20]);
        let raw_correlation_id = read_u64(&header[20..28]);
        let control_len = read_u32(&header[28..32]) as usize;
        let payload_len = read_u32(&header[32..36]) as usize;

        let correlation_id = (raw_correlation_id != 0).then_some(raw_correlation_id);
        validate_flags_and_correlation(flags, correlation_id)?;
        validate_message_correlation(message_type, correlation_id)?;
        validate_lengths(control_len, payload_len, limits)?;

        // Lengths were checked above, before allocating either vector.
        let mut control = vec![0u8; control_len];
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut control).map_err(FrameError::Io)?;
        reader.read_exact(&mut payload).map_err(FrameError::Io)?;

        Ok(Self {
            message_type,
            flags,
            request_id,
            correlation_id,
            control,
            payload,
        })
    }
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
    ])
}

fn validate_flags_and_correlation(
    flags: u16,
    correlation_id: Option<u64>
) -> Result<(), FrameError> {
    if flags != IPC_FLAGS_V1 {
        return Err(FrameError::UnsupportedFlags(flags));
    }
    if correlation_id == Some(0) {
        return Err(FrameError::InvalidCorrelationId);
    }
    Ok(())
}

fn validate_message_correlation(
    message_type: MessageType,
    correlation_id: Option<u64>
) -> Result<(), FrameError> {
    if
        matches!(message_type, MessageType::CapabilityRequest | MessageType::CapabilityResult) &&
        correlation_id.is_none()
    {
        return Err(FrameError::MissingCorrelationId(message_type));
    }
    Ok(())
}

fn validate_lengths(
    control_len: usize,
    payload_len: usize,
    limits: FrameLimits
) -> Result<(), FrameError> {
    if control_len > limits.max_control_bytes {
        return Err(FrameError::ControlTooLarge {
            actual: control_len,
            limit: limits.max_control_bytes,
        });
    }
    if payload_len > limits.max_payload_bytes {
        return Err(FrameError::PayloadTooLarge {
            actual: payload_len,
            limit: limits.max_payload_bytes,
        });
    }
    let body_len = control_len.checked_add(payload_len).ok_or(FrameError::LengthOverflow)?;
    let frame_len = IPC_HEADER_LEN.checked_add(body_len).ok_or(FrameError::LengthOverflow)?;
    if frame_len > limits.max_frame_bytes {
        return Err(FrameError::FrameTooLarge { actual: frame_len, limit: limits.max_frame_bytes });
    }
    // The lengths are encoded as u32; reject unrepresentable sizes before writing or allocating.
    if control_len > (u32::MAX as usize) || payload_len > (u32::MAX as usize) {
        return Err(FrameError::LengthOverflow);
    }
    Ok(())
}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    InvalidMagic,
    UnsupportedVersion {
        major: u16,
        minor: u16,
    },
    UnknownMessageType(u16),
    UnsupportedFlags(u16),
    InvalidCorrelationId,
    MissingCorrelationId(MessageType),
    ControlTooLarge {
        actual: usize,
        limit: usize,
    },
    PayloadTooLarge {
        actual: usize,
        limit: usize,
    },
    FrameTooLarge {
        actual: usize,
        limit: usize,
    },
    LengthOverflow,
    ControlEncode(serde_json::Error),
    ControlDecode(serde_json::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "IPC I/O error: {error}"),
            Self::InvalidMagic => f.write_str("invalid IPC frame magic"),
            Self::UnsupportedVersion { major, minor } => {
                write!(
                    f,
                    "unsupported IPC version {major}.{minor}; expected {IPC_MAJOR_VERSION}.{IPC_MINOR_VERSION}"
                )
            }
            Self::UnknownMessageType(value) => write!(f, "unknown IPC message type {value}"),
            Self::UnsupportedFlags(value) => write!(f, "unsupported IPC flags 0x{value:04x}"),
            Self::InvalidCorrelationId =>
                f.write_str("IPC correlation id must be non-zero when present"),
            Self::MissingCorrelationId(message_type) =>
                write!(f, "IPC message {message_type:?} requires a correlation id"),
            Self::ControlTooLarge { actual, limit } =>
                write!(f, "IPC control data too large: {actual} bytes (limit {limit})"),
            Self::PayloadTooLarge { actual, limit } =>
                write!(f, "IPC payload too large: {actual} bytes (limit {limit})"),
            Self::FrameTooLarge { actual, limit } =>
                write!(f, "IPC frame body too large: {actual} bytes (limit {limit})"),
            Self::LengthOverflow => f.write_str("IPC frame length cannot be represented"),
            Self::ControlEncode(error) => write!(f, "could not encode IPC control data: {error}"),
            Self::ControlDecode(error) => write!(f, "could not decode IPC control data: {error}"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::ControlEncode(error) | Self::ControlDecode(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn small_limits() -> FrameLimits {
        FrameLimits { max_control_bytes: 64, max_payload_bytes: 128, max_frame_bytes: 192 }
    }

    #[test]
    fn frame_round_trips_control_binary_payload_and_correlation() {
        let mut frame = IpcFrame::with_control(
            MessageType::Invoke,
            7,
            &(InvokeControl {
                operation: InvokeOperation::HandleEndpoint,
                remaining_budget_ms: 1200,
                operation_id: 0,
            })
        ).unwrap();
        frame.correlation_id = Some(42);
        frame.payload = vec![0, 1, 2, 0xff, 0, 0x7f];

        let mut bytes = Cursor::new(Vec::new());
        frame.write_to(&mut bytes, FrameLimits::default()).unwrap();
        bytes.set_position(0);
        let decoded = IpcFrame::read_from(&mut bytes, FrameLimits::default()).unwrap();

        assert_eq!(decoded, frame);
        let control: InvokeControl = decoded.decode_control().unwrap();
        assert_eq!(control.operation, InvokeOperation::HandleEndpoint);
        assert_eq!(control.remaining_budget_ms, 1200);
    }

    #[test]
    fn capability_messages_require_call_correlation() {
        let request = IpcFrame::new(MessageType::CapabilityRequest, 3);
        let mut output = Cursor::new(Vec::new());
        assert!(
            matches!(
                request.write_to(&mut output, small_limits()),
                Err(FrameError::MissingCorrelationId(MessageType::CapabilityRequest))
            )
        );
        assert!(output.get_ref().is_empty());

        let mut response = IpcFrame::new(MessageType::CapabilityResult, 3);
        response.correlation_id = Some(9);
        response.write_to(&mut output, small_limits()).unwrap();
    }

    #[test]
    fn in_flight_payload_budget_is_bounded_and_released_by_raii() {
        let budget = InFlightPayloadBudget::new(10);
        let first = budget.reserve(6).unwrap();
        assert_eq!(first.reserved_bytes(), 6);
        assert_eq!(budget.used_bytes(), 6);
        assert!(
            matches!(
                budget.reserve(5),
                Err(PayloadBudgetError::LimitExceeded {
                    requested_bytes: 5,
                    used_bytes: 6,
                    maximum_bytes: 10,
                })
            )
        );
        drop(first);
        assert_eq!(budget.used_bytes(), 0);
        let second = budget.reserve(10).unwrap();
        assert_eq!(budget.used_bytes(), 10);
        drop(second);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn rejects_control_and_payload_limits_before_writing() {
        let mut frame = IpcFrame::new(MessageType::Invoke, 1);
        frame.control = vec![b'x'; 65];
        let mut output = Cursor::new(Vec::new());
        assert!(
            matches!(
                frame.write_to(&mut output, small_limits()),
                Err(FrameError::ControlTooLarge { .. })
            )
        );
        assert!(output.get_ref().is_empty());

        frame.control = b"{}".to_vec();
        frame.payload = vec![0; 129];
        assert!(
            matches!(
                frame.write_to(&mut output, small_limits()),
                Err(FrameError::PayloadTooLarge { .. })
            )
        );
        assert!(output.get_ref().is_empty());
    }

    #[test]
    fn rejects_declared_oversized_control_before_allocating_or_reading_body() {
        let mut header = valid_header(MessageType::Invoke as u16);
        header[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut input = Cursor::new(header.to_vec());
        assert!(
            matches!(
                IpcFrame::read_from(&mut input, small_limits()),
                Err(FrameError::ControlTooLarge { .. })
            )
        );
    }

    #[test]
    fn rejects_bad_magic_version_message_type_and_reserved_flags() {
        let mut header = valid_header(MessageType::Invoke as u16);
        header[0] = b'X';
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(header.to_vec()), small_limits()),
                Err(FrameError::InvalidMagic)
            )
        );

        let mut header = valid_header(MessageType::Invoke as u16);
        header[4..6].copy_from_slice(&(2u16).to_le_bytes());
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(header.to_vec()), small_limits()),
                Err(FrameError::UnsupportedVersion { .. })
            )
        );

        let mut header = valid_header(999);
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(header.to_vec()), small_limits()),
                Err(FrameError::UnknownMessageType(999))
            )
        );

        header = valid_header(MessageType::Invoke as u16);
        header[10..12].copy_from_slice(&(1u16).to_le_bytes());
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(header.to_vec()), small_limits()),
                Err(FrameError::UnsupportedFlags(1))
            )
        );
    }

    #[test]
    fn rejects_combined_frame_body_limit() {
        let limits = FrameLimits {
            max_control_bytes: 64,
            max_payload_bytes: 128,
            max_frame_bytes: IPC_HEADER_LEN + 10,
        };
        let mut frame = IpcFrame::new(MessageType::Invoke, 1);
        frame.control = vec![b'x'; 6];
        frame.payload = vec![0; 5];
        let mut output = Cursor::new(Vec::new());
        assert!(
            matches!(frame.write_to(&mut output, limits), Err(FrameError::FrameTooLarge { actual, limit }) if actual == IPC_HEADER_LEN + 11 && limit == IPC_HEADER_LEN + 10)
        );
        assert!(output.get_ref().is_empty());
    }

    #[test]
    fn rejects_capability_message_without_correlation_on_receive() {
        let mut header = valid_header(MessageType::CapabilityRequest as u16);
        header[20..28].copy_from_slice(&(0u64).to_le_bytes());
        let mut input = Cursor::new(header.to_vec());
        assert!(
            matches!(
                IpcFrame::read_from(&mut input, small_limits()),
                Err(FrameError::MissingCorrelationId(MessageType::CapabilityRequest))
            )
        );
    }

    #[test]
    fn rejects_truncated_header_and_payload() {
        let truncated_header = [0u8; 12];
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(truncated_header), small_limits()),
                Err(FrameError::Io(_))
            )
        );

        let mut frame = IpcFrame::new(MessageType::Invoke, 1);
        frame.payload = vec![1, 2, 3];
        let mut bytes = Cursor::new(Vec::new());
        frame.write_to(&mut bytes, small_limits()).unwrap();
        let mut truncated = bytes.into_inner();
        truncated.pop();
        assert!(
            matches!(
                IpcFrame::read_from(&mut Cursor::new(truncated), small_limits()),
                Err(FrameError::Io(_))
            )
        );
    }

    #[test]
    fn rejects_malformed_control_json_when_decoding_typed_message() {
        let mut frame = IpcFrame::new(MessageType::Invoke, 1);
        frame.control = b"{broken".to_vec();
        assert!(
            matches!(frame.decode_control::<InvokeControl>(), Err(FrameError::ControlDecode(_)))
        );
    }

    fn valid_header(message_type: u16) -> [u8; IPC_HEADER_LEN] {
        let mut header = [0u8; IPC_HEADER_LEN];
        header[0..4].copy_from_slice(&IPC_MAGIC);
        header[4..6].copy_from_slice(&IPC_MAJOR_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&IPC_MINOR_VERSION.to_le_bytes());
        header[8..10].copy_from_slice(&message_type.to_le_bytes());
        header[10..12].copy_from_slice(&(0u16).to_le_bytes());
        header[12..20].copy_from_slice(&(1u64).to_le_bytes());
        header[20..28].copy_from_slice(&(0u64).to_le_bytes());
        header[28..32].copy_from_slice(&(2u32).to_le_bytes());
        header[32..36].copy_from_slice(&(0u32).to_le_bytes());
        header
    }
}
