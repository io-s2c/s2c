package io.s2c.network.message.reader;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.protobuf.InvalidProtocolBufferException;

import io.s2c.error.MessageTooLargeException;
import io.s2c.model.messages.S2CMessage;

public interface S2CMessageReader {
  public S2CMessage readNextMessage(DataInputStream din) throws IOException,
  EOFException,
  MessageTooLargeException,
  InvalidProtocolBufferException;
  
  public static S2CMessageReader create(int maxMessageSize) {
    return new S2CMessageReaderImpl(maxMessageSize);
  }
  
  public static S2CMessageReader wrapForTest(S2CMessageReader s2cMessageReader, Supplier<Integer> failEvery, Set<Failure> failures, Consumer<S2CMessage> messageInspector) {
    return new S2CMessageReaderTestWrapper((S2CMessageReaderImpl) s2cMessageReader, failEvery, failures, messageInspector);
  }
}
