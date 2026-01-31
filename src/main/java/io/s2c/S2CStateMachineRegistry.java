package io.s2c;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class S2CStateMachineRegistry {

  private final Map<String, S2CStateMachine> stateMachines = new ConcurrentHashMap<>();
  private final S2CNode s2cNode;
  private final SubmitFunction submitFunction;
  private final Supplier<Long> sequenceNumberSupplier;

  public S2CStateMachineRegistry(S2CNode s2cNode, SubmitFunction submitFunction, Supplier<Long> sequenceNumberSupplier) {
    this.s2cNode = s2cNode;
    this.submitFunction = submitFunction;
    this.sequenceNumberSupplier = sequenceNumberSupplier;
  }

  public S2CStateMachine get(String name) {
    return stateMachines.get(name);

  }

  public Set<S2CStateMachine> getAll() {
    return stateMachines.values().stream().collect(Collectors.toSet());

  }

  public <T extends S2CStateMachine> T createAndRegister(String name, Supplier<T> factory) {

    AtomicReference<T> stateMachineReference = new AtomicReference<>();

    stateMachines.computeIfAbsent(name, k -> {
      stateMachineReference.set(factory.get());
      stateMachineReference.get()
          .init(s2cNode.s2cGroupId(), s2cNode.nodeIdentity(), sequenceNumberSupplier, name, submitFunction);
      return stateMachineReference.get();
    });

    return stateMachineReference.get();

  }
}
