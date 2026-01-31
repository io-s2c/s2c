package io.s2c.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import io.s2c.error.ConnectTimeoutException;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.UnknownHostException;

class S2CConnection implements AutoCloseable {

  private final Socket socket;
  private final int connectTimeoutMs;
  private final NodeIdentity serverNodeIdentity;

  private InputStream in;
  private OutputStream out;
  private boolean connected;

  private S2CConnection(NodeIdentity serverNodeIdentity, int connectTimeoutMs) {
    this.socket = new Socket();
    this.serverNodeIdentity = serverNodeIdentity;
    this.connectTimeoutMs = connectTimeoutMs;
  }

  private void connect() throws IOException {
    this.socket.connect(
        new InetSocketAddress(serverNodeIdentity.getAddress(), serverNodeIdentity.getPort()),
        connectTimeoutMs);
  }

  public InputStream in() throws IOException {
    return socket.getInputStream();
  }

  public OutputStream out() throws IOException {
    return socket.getOutputStream();
  }
  
  public boolean isConnected() {
    return socket.isConnected();
  }
  
  public boolean isClosed() {
    return socket.isClosed();
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }

  public static S2CConnection of(NodeIdentity serverNodeIdentity, int connectTimeoutMs)
      throws IOException, UnknownHostException, ConnectTimeoutException {
    S2CConnection s2cConnection = new S2CConnection(serverNodeIdentity, connectTimeoutMs);
    try {
      s2cConnection.connect();
      return s2cConnection;
    }
    catch (IOException e) {
      // This won't throw because socket should already be closed here
      s2cConnection.close();
      if (e instanceof java.net.UnknownHostException ex) {
        throw new UnknownHostException(ex);
      }
      if (e instanceof SocketTimeoutException ex) {
        throw new ConnectTimeoutException(ex);
      }
      throw e;
    }
  }

}
