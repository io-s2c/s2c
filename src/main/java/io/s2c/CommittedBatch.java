package io.s2c;
import java.util.Set;

import io.s2c.StateRequestHandler.TraceableStateRequest;
import io.s2c.model.messages.StateRequest.StateRequestType;

public record CommittedBatch(
    Set<TraceableStateRequest> batch,
    StateRequestType requestsType, long commitIndex) {
}
