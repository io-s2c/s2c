package io.s2c.network.error;

import java.io.IOException;

public class ClientException extends Exception {

  public ClientException(IOException cause) {
    super(cause);
  }

  public ClientException(InterruptedException cause) {
    super(cause);
  }

  public ClientException(ClientStoppedException cause) {
    super(cause);
  }
  
  public ClientException(ClientNotConnectedException cause) {
    super(cause);
  }


  public void throwCause() throws IOException, InterruptedException,
  ClientStoppedException, ClientNotConnectedException {
    switch (getCause()) {
    case IOException e -> throw e;
    case InterruptedException e -> throw e;
    case ClientStoppedException e -> throw e;
    case ClientNotConnectedException e -> throw e;
    default -> {
      // Ignore
    }
    }
  }
}
