package io.s2c.network;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import io.s2c.model.state.NodeIdentity;

record Client(
    Socket socket,
    InputStream in,
    OutputStream out,
    NodeIdentity nodeIdentity
    ) {
}