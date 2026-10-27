//! A protocol-level fake worker. This deliberately runs in a thread over a socket pair: it tests
//! real stream framing and correlation without spawning a child process or enabling a sandbox.

#![cfg(unix)]
use PluginIpc::{
    CapabilityKind,
    CapabilityRequestControl,
    CapabilityResultControl,
    FrameLimits,
    HelloControl,
    IpcFrame,
    InvokeControl,
    InvokeOperation,
    MessageType,
    ResultControl,
    IPC_MAJOR_VERSION,
    IPC_MINOR_VERSION,
};
use std::io;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

#[test]
fn fake_worker_handles_invoke_and_correlated_database_capability_round_trip() -> io::Result<()> {
    let (mut host_stream, mut worker_stream) = UnixStream::pair()?;
    let timeout = Some(Duration::from_secs(5));
    host_stream.set_read_timeout(timeout)?;
    host_stream.set_write_timeout(timeout)?;
    worker_stream.set_read_timeout(timeout)?;
    worker_stream.set_write_timeout(timeout)?;
    let worker = thread::spawn(move || {
        let limits = FrameLimits::default();
        let hello = IpcFrame::read_from(&mut worker_stream, limits).expect("read Hello");
        assert_eq!(hello.message_type, MessageType::Hello);
        let hello_control: HelloControl = hello.decode_control().expect("decode Hello control");
        assert_eq!(hello_control.protocol_major, IPC_MAJOR_VERSION);
        assert_eq!(hello_control.protocol_minor, IPC_MINOR_VERSION);
        let hello_ack = IpcFrame::with_control(
            MessageType::Hello,
            hello.request_id,
            &(HelloControl {
                protocol_major: IPC_MAJOR_VERSION,
                protocol_minor: IPC_MINOR_VERSION,
                worker_build: "fake-worker-test".to_owned(),
            })
        ).expect("encode Hello ack");
        hello_ack.write_to(&mut worker_stream, limits).expect("write Hello ack");

        let invoke = IpcFrame::read_from(&mut worker_stream, limits).expect("read Invoke");
        assert_eq!(invoke.message_type, MessageType::Invoke);
        assert_eq!(invoke.request_id, 77);
        let invoke_control: InvokeControl = invoke.decode_control().expect("decode Invoke control");
        assert_eq!(invoke_control.operation, InvokeOperation::HandleEndpoint);
        assert_eq!(invoke_control.remaining_budget_ms, 1500);
        assert_eq!(invoke.payload.as_slice(), br#"{"endpoint":"/health"}"#);

        // The capability request has its own request_id and points back to the logical Invoke.
        let mut capability = IpcFrame::with_control(
            MessageType::CapabilityRequest,
            901,
            &(CapabilityRequestControl {
                operation_id: 77,
                capability: CapabilityKind::DatabaseExecute,
            })
        ).expect("encode CapabilityRequest");
        capability.correlation_id = Some(invoke.request_id);
        capability.payload = b"SELECT 1".to_vec();
        capability.write_to(&mut worker_stream, limits).expect("write CapabilityRequest");

        // The host's response is matched by capability request_id and retains call correlation.
        let capability_result = IpcFrame::read_from(&mut worker_stream, limits).expect(
            "read CapabilityResult"
        );
        assert_eq!(capability_result.message_type, MessageType::CapabilityResult);
        assert_eq!(capability_result.request_id, 901);
        assert_eq!(capability_result.correlation_id, Some(77));
        let control: CapabilityResultControl = capability_result
            .decode_control()
            .expect("decode capability result");
        assert!(control.success);
        assert_eq!(capability_result.payload.as_slice(), br#"{"rows":[1]}"#);

        let mut result = IpcFrame::with_control(
            MessageType::Result,
            invoke.request_id,
            &(ResultControl { success: true, error_code: None, error_message: None })
        ).expect("encode Result");
        result.payload = br#"{"status":"ok"}"#.to_vec();
        result.write_to(&mut worker_stream, limits).expect("write Result");
    });

    let limits = FrameLimits::default();
    let hello = IpcFrame::with_control(
        MessageType::Hello,
        1,
        &(HelloControl {
            protocol_major: IPC_MAJOR_VERSION,
            protocol_minor: IPC_MINOR_VERSION,
            worker_build: "fake-worker-test".to_owned(),
        })
    ).expect("encode Hello");
    hello.write_to(&mut host_stream, limits).expect("write Hello");
    let hello_ack = IpcFrame::read_from(&mut host_stream, limits).expect("read Hello ack");
    assert_eq!(hello_ack.message_type, MessageType::Hello);
    let hello_ack_control: HelloControl = hello_ack.decode_control().expect("decode Hello ack");
    assert_eq!(hello_ack_control.worker_build, "fake-worker-test");

    // This fake worker shares the real stream framing but does not spawn a child process.
    let mut invoke = IpcFrame::with_control(
        MessageType::Invoke,
        77,
        &(InvokeControl {
            operation: InvokeOperation::HandleEndpoint,
            remaining_budget_ms: 1500,
            operation_id: 0,
        })
    ).expect("encode Invoke");
    invoke.payload = br#"{"endpoint":"/health"}"#.to_vec();
    invoke.write_to(&mut host_stream, limits).expect("write Invoke");

    let capability = IpcFrame::read_from(&mut host_stream, limits).expect("read CapabilityRequest");
    assert_eq!(capability.message_type, MessageType::CapabilityRequest);
    assert_eq!(capability.request_id, 901);
    assert_eq!(capability.correlation_id, Some(77));
    let capability_control: CapabilityRequestControl = capability
        .decode_control()
        .expect("decode capability request");
    assert_eq!(capability_control.operation_id, 77);
    assert_eq!(capability_control.capability, CapabilityKind::DatabaseExecute);
    assert_eq!(capability.payload.as_slice(), b"SELECT 1");

    let mut capability_result = IpcFrame::with_control(
        MessageType::CapabilityResult,
        capability.request_id,
        &(CapabilityResultControl { success: true, error_code: None, error_message: None })
    ).expect("encode CapabilityResult");
    capability_result.correlation_id = capability.correlation_id;
    capability_result.payload = br#"{"rows":[1]}"#.to_vec();
    capability_result.write_to(&mut host_stream, limits).expect("write CapabilityResult");

    let result = IpcFrame::read_from(&mut host_stream, limits).expect("read Result");
    assert_eq!(result.message_type, MessageType::Result);
    assert_eq!(result.request_id, 77);
    let control: ResultControl = result.decode_control().expect("decode result");
    assert!(control.success);
    assert_eq!(control.error_code, None);
    assert_eq!(result.payload.as_slice(), br#"{"status":"ok"}"#);

    worker.join().expect("fake worker panicked");
    Ok(())
}
