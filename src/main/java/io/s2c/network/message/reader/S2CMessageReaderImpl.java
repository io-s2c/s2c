package io.s2c.network.message.reader;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import io.s2c.error.MessageTooLargeException;
import io.s2c.model.messages.S2CMessage;

class S2CMessageReaderImpl implements S2CMessageReader {

  private final int maxMessageSize;
  private final byte[] buffer;

  CodedInputStream cin;

  S2CMessageReaderImpl(int maxMessageSize) {
    this.maxMessageSize = maxMessageSize;
    this.buffer = new byte[maxMessageSize];
  }

  public S2CMessage readNextMessage(DataInputStream din) throws IOException,
  EOFException,
  MessageTooLargeException,
  InvalidProtocolBufferException {
    int msgSize = din.readInt();
    if (msgSize > maxMessageSize) {
      din.skipNBytes(msgSize);
      throw new MessageTooLargeException(
          "Message size %d too large. Max message size is: %s"
          .formatted(msgSize, maxMessageSize));
    }
    din.readFully(buffer, 0, msgSize);
    cin = CodedInputStream.newInstance(buffer, 0, msgSize);
    S2CMessage message = S2CMessage.parseFrom(cin);
    return message;
  }
}
