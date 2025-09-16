/*
 * Copyright 2019 fsyncd, Berlin, Germany.
 * Copyright 2025 tokio-vsock contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(any(target_os = "linux", target_os = "android"))]
use tokio_vsock::VMADDR_CID_LOCAL;
use tokio_vsock::{VsockAddr, VsockDatagram};

/// Test basic datagram send/receive functionality
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_datagram_send_recv() {
    const MSG: &[u8] = b"hello vsock datagram";
    const SERVER_PORT: u32 = 9001;

    // Create server socket
    let server_addr = VsockAddr::new(VMADDR_CID_LOCAL, SERVER_PORT);
    let server = VsockDatagram::bind(server_addr)
        .await
        .expect("Failed to bind server");

    // Create client socket
    let client_addr = VsockAddr::new(VMADDR_CID_LOCAL, 0); // Port 0 for auto-assignment
    let client = VsockDatagram::bind(client_addr)
        .await
        .expect("Failed to bind client");

    // Send message from client to server
    let bytes_sent = client
        .send_to(MSG, server_addr)
        .await
        .expect("Failed to send message");
    assert_eq!(bytes_sent, MSG.len());

    // Receive message on server
    let mut buf = vec![0u8; 1024];
    let (bytes_received, sender_addr) = server
        .recv_from(&mut buf)
        .await
        .expect("Failed to receive message");

    assert_eq!(bytes_received, MSG.len());
    assert_eq!(&buf[..bytes_received], MSG);

    // Verify sender address matches client's local address
    let client_local_addr = client
        .local_addr()
        .expect("Failed to get client local address");
    assert_eq!(sender_addr.cid(), client_local_addr.cid());
    assert_eq!(sender_addr.port(), client_local_addr.port());
}

/// Test connected datagram sockets
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_connected_datagram() {
    const MSG1: &[u8] = b"hello from client";
    const MSG2: &[u8] = b"hello from server";
    const SERVER_PORT: u32 = 9002;

    // Create server socket
    let server_addr = VsockAddr::new(VMADDR_CID_LOCAL, SERVER_PORT);
    let server = VsockDatagram::bind(server_addr)
        .await
        .expect("Failed to bind server");

    // Create client socket and connect to server
    let client_addr = VsockAddr::new(VMADDR_CID_LOCAL, 0);
    let client = VsockDatagram::bind(client_addr)
        .await
        .expect("Failed to bind client");
    client
        .connect(server_addr)
        .await
        .expect("Failed to connect");

    // Get client's actual address for server to connect back
    let client_local_addr = client.local_addr().expect("Failed to get client address");

    // Send message from client using connected send
    let bytes_sent = client.send(MSG1).await.expect("Failed to send message");
    assert_eq!(bytes_sent, MSG1.len());

    // Receive on server
    let mut buf = vec![0u8; 1024];
    let (bytes_received, sender_addr) = server
        .recv_from(&mut buf)
        .await
        .expect("Failed to receive message");

    assert_eq!(bytes_received, MSG1.len());
    assert_eq!(&buf[..bytes_received], MSG1);
    assert_eq!(sender_addr.port(), client_local_addr.port());

    // Connect server back to client and send response
    server
        .connect(sender_addr)
        .await
        .expect("Failed to connect server to client");
    let bytes_sent = server.send(MSG2).await.expect("Failed to send response");
    assert_eq!(bytes_sent, MSG2.len());

    // Receive response on client
    let mut buf = vec![0u8; 1024];
    let bytes_received = client
        .recv(&mut buf)
        .await
        .expect("Failed to receive response");

    assert_eq!(bytes_received, MSG2.len());
    assert_eq!(&buf[..bytes_received], MSG2);
}

/// Test bidirectional communication with multiple messages
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_datagram_echo_server() {
    const MESSAGES: &[&[u8]] = &[
        b"message 1",
        b"a longer message with more content",
        b"short",
        b"final message",
    ];
    const SERVER_PORT: u32 = 9003;

    // Create server socket
    let server_addr = VsockAddr::new(VMADDR_CID_LOCAL, SERVER_PORT);
    let server = VsockDatagram::bind(server_addr)
        .await
        .expect("Failed to bind server");

    // Create client socket
    let client_addr = VsockAddr::new(VMADDR_CID_LOCAL, 0);
    let client = VsockDatagram::bind(client_addr)
        .await
        .expect("Failed to bind client");

    // Spawn server task to echo messages
    let server_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        for _ in 0..MESSAGES.len() {
            let (bytes_received, sender_addr) = server
                .recv_from(&mut buf)
                .await
                .expect("Failed to receive message");

            // Echo the message back
            let bytes_sent = server
                .send_to(&buf[..bytes_received], sender_addr)
                .await
                .expect("Failed to echo message");

            assert_eq!(bytes_sent, bytes_received);
        }
    });

    // Send messages and verify echoes
    for &message in MESSAGES {
        // Send message
        let bytes_sent = client
            .send_to(message, server_addr)
            .await
            .expect("Failed to send message");
        assert_eq!(bytes_sent, message.len());

        // Receive echo
        let mut buf = vec![0u8; 1024];
        let (bytes_received, _) = client
            .recv_from(&mut buf)
            .await
            .expect("Failed to receive echo");

        assert_eq!(bytes_received, message.len());
        assert_eq!(&buf[..bytes_received], message);
    }

    // Wait for server to finish
    server_handle.await.expect("Server task failed");
}

/// Test error conditions
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_datagram_errors() {
    // Test binding to an already bound port
    let addr = VsockAddr::new(VMADDR_CID_LOCAL, 9004);
    let _socket1 = VsockDatagram::bind(addr)
        .await
        .expect("Failed to bind first socket");

    // This should fail because the port is already in use
    let result = VsockDatagram::bind(addr).await;
    assert!(result.is_err(), "Expected error when binding to used port");

    // Test sending to non-existent address
    let client = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0))
        .await
        .expect("Failed to bind client");

    let nonexistent_addr = VsockAddr::new(VMADDR_CID_LOCAL, 65535);
    let result = client.send_to(b"test", nonexistent_addr).await;
    // This might succeed immediately but fail at the network level,
    // so we just verify it doesn't panic
    let _ = result;
}

/// Test local and peer address methods
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_datagram_addresses() {
    let server_addr = VsockAddr::new(VMADDR_CID_LOCAL, 9005);
    let server = VsockDatagram::bind(server_addr)
        .await
        .expect("Failed to bind server");

    // Test local address
    let local_addr = server.local_addr().expect("Failed to get local address");
    assert_eq!(local_addr.cid(), VMADDR_CID_LOCAL);
    assert_eq!(local_addr.port(), 9005);

    // Test peer address before connection (should fail)
    let result = server.peer_addr();
    assert!(
        result.is_err(),
        "Expected error getting peer address before connection"
    );

    // Create client and connect
    let client = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0))
        .await
        .expect("Failed to bind client");
    client
        .connect(server_addr)
        .await
        .expect("Failed to connect");

    // Test peer address after connection
    let peer_addr = client.peer_addr().expect("Failed to get peer address");
    assert_eq!(peer_addr.cid(), VMADDR_CID_LOCAL);
    assert_eq!(peer_addr.port(), 9005);
}

/// Test large message handling
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn test_large_datagram() {
    const MESSAGE_SIZE: usize = 8192; // 8KB message
    const SERVER_PORT: u32 = 9006;

    // Create a large message
    let large_message: Vec<u8> = (0..MESSAGE_SIZE).map(|i| (i % 256) as u8).collect();

    // Create server socket
    let server_addr = VsockAddr::new(VMADDR_CID_LOCAL, SERVER_PORT);
    let server = VsockDatagram::bind(server_addr)
        .await
        .expect("Failed to bind server");

    // Create client socket
    let client = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0))
        .await
        .expect("Failed to bind client");

    // Send large message
    let bytes_sent = client
        .send_to(&large_message, server_addr)
        .await
        .expect("Failed to send large message");
    assert_eq!(bytes_sent, MESSAGE_SIZE);

    // Receive large message
    let mut buf = vec![0u8; MESSAGE_SIZE + 1024]; // Extra space
    let (bytes_received, _) = server
        .recv_from(&mut buf)
        .await
        .expect("Failed to receive large message");

    assert_eq!(bytes_received, MESSAGE_SIZE);
    assert_eq!(&buf[..bytes_received], &large_message);
}
