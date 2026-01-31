package io.s2c.network.message.reader;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.s2c.error.MessageTooLargeException;
import io.s2c.model.messages.S2CMessage;

class S2CMessageReaderTestWrapper implements S2CMessageReader {

  List<Failure> failures = new LinkedList<>();

  private final Logger logger = LoggerFactory.getLogger(S2CMessageReaderTestWrapper.class);

  private final S2CMessageReaderImpl s2cMessageReaderImpl;

  private int readingCounter = 1;

  private final Supplier<Integer> failEvery;
  
  private final Consumer<S2CMessage> messageInspector;
  

  public S2CMessageReaderTestWrapper(S2CMessageReaderImpl s2cMessageReaderImpl,
      Supplier<Integer> failEvery,
      Set<Failure> failures, Consumer<S2CMessage> messageInspector) {
    this.s2cMessageReaderImpl = s2cMessageReaderImpl;
    this.failEvery = failEvery;
    this.failures.addAll(failures);
    this.messageInspector = messageInspector;
  }

  @Override
  public S2CMessage readNextMessage(DataInputStream din)
      throws IOException, EOFException, MessageTooLargeException, InvalidProtocolBufferException {

    S2CMessage message = null;
    boolean shouldFail = false;
    
    // We never deliver null message
    while (message == null) {

      message = s2cMessageReaderImpl.readNextMessage(din);
      
      messageInspector.accept(message);
      
      shouldFail = false;

      if (message.hasHandshake() || message.hasFollowResponse()) {
        return message;
      }

      readingCounter++;

      
      if (failEvery.get() > 0 && readingCounter % failEvery.get() == 0) {
        shouldFail = true;
      }

      if (!shouldFail) {
        return message;
      }
      
      if (failures.isEmpty()) {
        return message;
      }
      
      Failure currentFailure = failures.removeFirst();
      failures.add(currentFailure);
      
      if (currentFailure == Failure.IO_EXCEPTION) {
        logger.trace("Throwing IOException");
        throw new IOException();
      } else if (currentFailure == Failure.DROP) {
        logger.trace("Dropping message");
        message = null;
      }
    }
    return message;
  }
}
