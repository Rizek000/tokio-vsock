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

use std::io::{Error, Result};
use std::mem::{self, size_of};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::task::{Context, Poll};

use futures::ready;
use libc::*;
use tokio::io::unix::AsyncFd;

use crate::VsockAddr;

/// Low-level VSOCK datagram socket wrapper
#[derive(Debug)]
struct VsockDatagramSocket {
    fd: RawFd,
}

impl VsockDatagramSocket {
    /// Create and bind a VSOCK datagram socket
    fn bind(addr: VsockAddr) -> Result<Self> {
        unsafe {
            // Create AF_VSOCK, SOCK_DGRAM socket
            let fd = socket(AF_VSOCK, SOCK_DGRAM, 0);
            if fd < 0 {
                return Err(Error::last_os_error());
            }

            // Set close-on-exec flag
            if fcntl(fd, F_SETFD, FD_CLOEXEC) < 0 {
                let _ = close(fd);
                return Err(Error::last_os_error());
            }

            // Convert VsockAddr to sockaddr_vm
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            #[cfg(target_os = "macos")]
            {
                sockaddr.svm_family = AF_VSOCK as u8;
            }
            #[cfg(not(target_os = "macos"))]
            {
                sockaddr.svm_family = AF_VSOCK as u16;
            }
            sockaddr.svm_port = addr.port();
            sockaddr.svm_cid = addr.cid();

            // Bind to address
            let result = bind(
                fd,
                &sockaddr as *const _ as *const sockaddr,
                size_of::<sockaddr_vm>() as socklen_t,
            );

            if result < 0 {
                let _ = close(fd);
                return Err(Error::last_os_error());
            }

            Ok(Self { fd })
        }
    }

    /// Send data to a specific address
    fn send_to(&self, buf: &[u8], addr: &VsockAddr) -> Result<usize> {
        unsafe {
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            #[cfg(target_os = "macos")]
            {
                sockaddr.svm_family = AF_VSOCK as u8;
            }
            #[cfg(not(target_os = "macos"))]
            {
                sockaddr.svm_family = AF_VSOCK as u16;
            }
            sockaddr.svm_port = addr.port();
            sockaddr.svm_cid = addr.cid();

            let result = sendto(
                self.fd,
                buf.as_ptr() as *const c_void,
                buf.len(),
                0,
                &sockaddr as *const _ as *const sockaddr,
                size_of::<sockaddr_vm>() as socklen_t,
            );

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(result as usize)
            }
        }
    }

    /// Receive data and return sender address
    fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, VsockAddr)> {
        unsafe {
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            let mut sockaddr_len = size_of::<sockaddr_vm>() as socklen_t;

            let result = recvfrom(
                self.fd,
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                0,
                &mut sockaddr as *mut _ as *mut sockaddr,
                &mut sockaddr_len,
            );

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                // Convert sockaddr_vm back to VsockAddr
                let addr = VsockAddr::new(sockaddr.svm_cid, sockaddr.svm_port);
                Ok((result as usize, addr))
            }
        }
    }

    /// Connect to a remote address (for connected UDP-style sockets)
    fn connect(&self, addr: &VsockAddr) -> Result<()> {
        unsafe {
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            #[cfg(target_os = "macos")]
            {
                sockaddr.svm_family = AF_VSOCK as u8;
            }
            #[cfg(not(target_os = "macos"))]
            {
                sockaddr.svm_family = AF_VSOCK as u16;
            }
            sockaddr.svm_port = addr.port();
            sockaddr.svm_cid = addr.cid();

            let result = connect(
                self.fd,
                &sockaddr as *const _ as *const sockaddr,
                size_of::<sockaddr_vm>() as socklen_t,
            );

            if result < 0 {
                let err = Error::last_os_error();
                // For SOCK_DGRAM, connect should complete immediately
                if err.raw_os_error() == Some(EINPROGRESS) {
                    // This shouldn't happen for SOCK_DGRAM, but handle it gracefully
                    return Ok(());
                }
                return Err(err);
            }

            Ok(())
        }
    }

    /// Send data to connected peer
    fn send(&self, buf: &[u8]) -> Result<usize> {
        unsafe {
            let result = send(self.fd, buf.as_ptr() as *const c_void, buf.len(), 0);

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(result as usize)
            }
        }
    }

    /// Receive data from connected peer
    fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        unsafe {
            let result = recv(self.fd, buf.as_mut_ptr() as *mut c_void, buf.len(), 0);

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(result as usize)
            }
        }
    }

    /// Set socket to non-blocking mode
    fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        unsafe {
            let flags = fcntl(self.fd, F_GETFL);
            if flags < 0 {
                return Err(Error::last_os_error());
            }

            let new_flags = if nonblocking {
                flags | O_NONBLOCK
            } else {
                flags & !O_NONBLOCK
            };

            if fcntl(self.fd, F_SETFL, new_flags) < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    /// Get local address
    fn local_addr(&self) -> Result<VsockAddr> {
        unsafe {
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            let mut sockaddr_len = size_of::<sockaddr_vm>() as socklen_t;

            let result = getsockname(
                self.fd,
                &mut sockaddr as *mut _ as *mut sockaddr,
                &mut sockaddr_len,
            );

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(VsockAddr::new(sockaddr.svm_cid, sockaddr.svm_port))
            }
        }
    }

    /// Get peer address (only valid for connected sockets)
    fn peer_addr(&self) -> Result<VsockAddr> {
        unsafe {
            let mut sockaddr: sockaddr_vm = mem::zeroed();
            let mut sockaddr_len = size_of::<sockaddr_vm>() as socklen_t;

            let result = getpeername(
                self.fd,
                &mut sockaddr as *mut _ as *mut sockaddr,
                &mut sockaddr_len,
            );

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(VsockAddr::new(sockaddr.svm_cid, sockaddr.svm_port))
            }
        }
    }
}

impl Drop for VsockDatagramSocket {
    fn drop(&mut self) {
        unsafe {
            let _ = close(self.fd);
        }
    }
}

impl AsFd for VsockDatagramSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

impl AsRawFd for VsockDatagramSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl FromRawFd for VsockDatagramSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl IntoRawFd for VsockDatagramSocket {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        mem::forget(self);
        fd
    }
}

/// An I/O object representing a VSOCK datagram socket.
///
/// A datagram socket provides connectionless, unreliable message transmission
/// over VSOCK. This is similar to UDP but operates over the VSOCK transport.
#[derive(Debug)]
pub struct VsockDatagram {
    inner: AsyncFd<VsockDatagramSocket>,
}

impl VsockDatagram {
    /// Create a new VSOCK datagram socket bound to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let addr = VsockAddr::new(VMADDR_CID_LOCAL, 0); // Port 0 for auto-assignment
    /// let socket = VsockDatagram::bind(addr).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bind(addr: VsockAddr) -> Result<Self> {
        let socket = VsockDatagramSocket::bind(addr)?;
        socket.set_nonblocking(true)?;

        Ok(Self {
            inner: AsyncFd::new(socket)?,
        })
    }

    /// Send data to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// let target = VsockAddr::new(VMADDR_CID_LOCAL, 1234);
    /// let bytes_sent = socket.send_to(b"hello world", target).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_to(&self, buf: &[u8], addr: VsockAddr) -> Result<usize> {
        loop {
            let mut guard = self.inner.writable().await?;

            match guard.try_io(|inner| inner.get_ref().send_to(buf, &addr)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Receive data from any sender.
    ///
    /// Returns the number of bytes received and the address of the sender.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 1234)).await?;
    /// let mut buf = vec![0u8; 1024];
    /// let (bytes_received, sender_addr) = socket.recv_from(&mut buf).await?;
    /// println!("Received {} bytes from {:?}", bytes_received, sender_addr);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, VsockAddr)> {
        loop {
            let mut guard = self.inner.readable().await?;

            match guard.try_io(|inner| inner.get_ref().recv_from(buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Connect this socket to a remote address.
    ///
    /// After connecting, `send()` and `recv()` can be used to send and receive
    /// data to/from the connected peer, and `send_to()` and `recv_from()` will
    /// only work with the connected peer.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// let target = VsockAddr::new(VMADDR_CID_LOCAL, 1234);
    /// socket.connect(target).await?;
    /// socket.send(b"hello").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(&self, addr: VsockAddr) -> Result<()> {
        // For SOCK_DGRAM, connect should complete immediately
        self.inner.get_ref().connect(&addr)
    }

    /// Send data to the connected peer.
    ///
    /// The socket must be connected using `connect()` before calling this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// socket.connect(VsockAddr::new(VMADDR_CID_LOCAL, 1234)).await?;
    /// let bytes_sent = socket.send(b"hello").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, buf: &[u8]) -> Result<usize> {
        loop {
            let mut guard = self.inner.writable().await?;

            match guard.try_io(|inner| inner.get_ref().send(buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Receive data from the connected peer.
    ///
    /// The socket must be connected using `connect()` before calling this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// socket.connect(VsockAddr::new(VMADDR_CID_LOCAL, 1234)).await?;
    /// let mut buf = vec![0u8; 1024];
    /// let bytes_received = socket.recv(&mut buf).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        loop {
            let mut guard = self.inner.readable().await?;

            match guard.try_io(|inner| inner.get_ref().recv(buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Get the local address that this socket is bound to.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// let local_addr = socket.local_addr()?;
    /// println!("Bound to: {:?}", local_addr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn local_addr(&self) -> Result<VsockAddr> {
        self.inner.get_ref().local_addr()
    }

    /// Get the remote address that this socket is connected to.
    ///
    /// This method will return an error if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_vsock::{VsockDatagram, VsockAddr, VMADDR_CID_LOCAL};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let socket = VsockDatagram::bind(VsockAddr::new(VMADDR_CID_LOCAL, 0)).await?;
    /// socket.connect(VsockAddr::new(VMADDR_CID_LOCAL, 1234)).await?;
    /// let peer_addr = socket.peer_addr()?;
    /// println!("Connected to: {:?}", peer_addr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn peer_addr(&self) -> Result<VsockAddr> {
        self.inner.get_ref().peer_addr()
    }

    /// Attempt to send data to the specified address without blocking.
    ///
    /// This method returns `Poll::Ready(Ok(n))` if the data was sent successfully,
    /// where `n` is the number of bytes sent. If the socket is not ready to send
    /// data, it returns `Poll::Pending`.
    pub fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        addr: VsockAddr,
    ) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().send_to(buf, &addr)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    /// Attempt to receive data without blocking.
    ///
    /// This method returns `Poll::Ready(Ok((n, addr)))` if data was received,
    /// where `n` is the number of bytes received and `addr` is the sender's address.
    /// If no data is available, it returns `Poll::Pending`.
    pub fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<(usize, VsockAddr)>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().recv_from(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    /// Attempt to send data to the connected peer without blocking.
    ///
    /// This method returns `Poll::Ready(Ok(n))` if the data was sent successfully,
    /// where `n` is the number of bytes sent. If the socket is not ready to send
    /// data, it returns `Poll::Pending`.
    pub fn poll_send(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().send(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    /// Attempt to receive data from the connected peer without blocking.
    ///
    /// This method returns `Poll::Ready(Ok(n))` if data was received,
    /// where `n` is the number of bytes received. If no data is available,
    /// it returns `Poll::Pending`.
    pub fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().recv(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsFd for VsockDatagram {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.get_ref().as_fd()
    }
}

impl AsRawFd for VsockDatagram {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.get_ref().as_raw_fd()
    }
}

impl FromRawFd for VsockDatagram {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = VsockDatagramSocket::from_raw_fd(fd);
        Self {
            inner: AsyncFd::new(socket).expect("Failed to create AsyncFd"),
        }
    }
}

impl IntoRawFd for VsockDatagram {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.inner.get_ref().as_raw_fd();
        mem::forget(self);
        fd
    }
}
