package com.amnesica.belugaproject.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JdkWebSocketServer {
  private final int port;
  private final String path;
  private ServerSocketChannel serverChannel;
  private final AtomicReference<WebSocketConnection> clientConnectionRef = new AtomicReference<>();
  private final CountDownLatch clientConnectedLatch = new CountDownLatch(1);
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private volatile boolean running = true;

  public JdkWebSocketServer(int port, String path) {
    this.port = port;
    this.path = path.startsWith("/") ? path : "/" + path;
  }

  public void start() throws IOException {
    serverChannel = ServerSocketChannel.open();
    serverChannel.bind(new InetSocketAddress(port));
    serverChannel.configureBlocking(true);

    executor.submit(() -> {
      try {
        while (running) {
          SocketChannel clientChannel = serverChannel.accept();
          handleClient(clientChannel);
        }
      } catch (IOException e) {
        if (running) {
          e.printStackTrace();
        }
      }
    });
  }

  private void handleClient(SocketChannel clientChannel) {
    executor.submit(() -> {
      try {
        // Read HTTP request
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        int bytesRead = clientChannel.read(buffer);
        if (bytesRead == -1) return;

        buffer.flip();
        String request = StandardCharsets.UTF_8.decode(buffer).toString();

        // Handle WebSocket upgrade
        if (request.contains("Upgrade: websocket")) {
          String key = extractWebSocketKey(request);
          String accept = generateWebSocketAccept(key);

          String response = "HTTP/1.1 101 Switching Protocols\r\n" +
              "Upgrade: websocket\r\n" +
              "Connection: Upgrade\r\n" +
              "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";

          clientChannel.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));

          // Create WebSocket connection
          WebSocketConnection connection = new WebSocketConnection(clientChannel);
          clientConnectionRef.set(connection);
          clientConnectedLatch.countDown();

          // Start reading messages
          connection.readMessages();
        }
      } catch (IOException e) {
        e.printStackTrace();
        try {
          clientChannel.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    });
  }

  public void sendToWebSocket(String message) throws IOException {
    WebSocketConnection connection = clientConnectionRef.get();
    if (connection == null || !connection.isConnected()) {
      throw new IllegalStateException("No connected WebSocket client.");
    }
    connection.sendMessage(message);
  }

  public boolean waitForClientConnected(long timeout, TimeUnit unit) throws InterruptedException {
    return clientConnectedLatch.await(timeout, unit);
  }

  public void stop() throws IOException {
    running = false;

    // Close all client connections first
    WebSocketConnection connection = clientConnectionRef.get();
    if (connection != null) {
      connection.close();
    }

    if (serverChannel != null) {
      serverChannel.close();
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private String extractWebSocketKey(String request) {
    for (String line : request.split("\r\n")) {
      if (line.startsWith("Sec-WebSocket-Key:")) {
        return line.substring("Sec-WebSocket-Key:".length()).trim();
      }
    }
    return null;
  }

  private String generateWebSocketAccept(String key) {
    try {
      String input = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
      MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
      byte[] hash = sha1.digest(input.getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to generate WebSocket accept key", e);
    }
  }

  private class WebSocketConnection {
    private final SocketChannel channel;
    private volatile boolean connected = true;

    public WebSocketConnection(SocketChannel channel) {
      this.channel = channel;
    }

    public boolean isConnected() {
      return connected && channel.isOpen();
    }

    public void readMessages() {
      executor.submit(() -> {
        try {
          ByteBuffer buffer = ByteBuffer.allocate(4096);
          while (connected && channel.isOpen()) {
            buffer.clear();
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) break;

            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            // Parse WebSocket frame
            if (data.length >= 2) {
              boolean fin = (data[0] & 0x80) != 0;
              byte opcode = (byte) (data[0] & 0x0F);
              boolean masked = (data[1] & 0x80) != 0;
              int payloadLength = data[1] & 0x7F;

              int maskingKeyStart = 2;
              if (payloadLength == 126) {
                maskingKeyStart = 4;
              } else if (payloadLength == 127) {
                maskingKeyStart = 10;
              }

              if (!masked) continue;

              byte[] maskingKey = new byte[4];
              System.arraycopy(data, maskingKeyStart, maskingKey, 0, 4);

              int payloadStart = maskingKeyStart + 4;
              if (payloadStart >= data.length) continue;

              byte[] payload = new byte[data.length - payloadStart];
              System.arraycopy(data, payloadStart, payload, 0, payload.length);

              // Unmask payload
              for (int i = 0; i < payload.length; i++) {
                payload[i] ^= maskingKey[i % 4];
              }

              if (opcode == 0x01) { // Text frame
                String message = new String(payload, StandardCharsets.UTF_8);
                // Echo back the message
                sendMessage(message);
              } else if (opcode == 0x08) { // Close frame
                connected = false;
                break;
              }
            }
          }
        } catch (IOException e) {
          // Connection closed
        } finally {
          connected = false;
          try {
            channel.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
          clientConnectionRef.compareAndSet(this, null);
        }
      });
    }

    public void sendMessage(String message) throws IOException {
      if (!connected || !channel.isOpen()) {
        throw new IOException("Connection is closed");
      }

      byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(10 + messageBytes.length);

      // FIN + Text frame opcode
      buffer.put((byte) 0x81);

      // Payload length
      if (messageBytes.length <= 125) {
        buffer.put((byte) messageBytes.length);
      } else if (messageBytes.length <= 65535) {
        buffer.put((byte) 126);
        buffer.putShort((short) messageBytes.length);
      } else {
        buffer.put((byte) 127);
        buffer.putLong(messageBytes.length);
      }

      // Payload
      buffer.put(messageBytes);
      buffer.flip();

      channel.write(buffer);
    }

    public void close() {
      connected = false;
      try {
        if (channel != null && channel.isOpen()) {
          // Send close frame
          ByteBuffer buffer = ByteBuffer.allocate(2);
          buffer.put((byte) 0x88); // FIN + Close opcode
          buffer.put((byte) 0x00); // No payload
          buffer.flip();
          channel.write(buffer);
          channel.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
