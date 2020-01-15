/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor;

import org.mule.runtime.api.util.Pair;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.internal.message.EventInternalContext;
import org.mule.runtime.core.internal.message.InternalEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Contains the {@link CompletableFuture}s for each event/blocking processor combination to that the caller thread may wait for it
 * and resume processing in the original thread.
 * 
 * @since 4.3
 */
public class BlockingExecutionContext implements EventInternalContext<BlockingExecutionContext> {

  /**
   * @param event
   * @return the {@link BlockingExecutionContext} form the event or a new one if there was none
   */
  public static BlockingExecutionContext from(CoreEvent event) {
    BlockingExecutionContext blockingExecutionContext =
        (BlockingExecutionContext) ((InternalEvent) event).<BlockingExecutionContext>getBlockingExecutionContext();

    if (blockingExecutionContext == null) {
      blockingExecutionContext = new BlockingExecutionContext();
      ((InternalEvent) event).setBlockingExecutionContext(blockingExecutionContext);
    }

    return blockingExecutionContext;
  }

  private final Map<Pair<ReactiveProcessor, String>, CompletableFuture<CoreEvent>> registry = new HashMap<>();

  @Override
  public BlockingExecutionContext copy() {
    return this;
  }

  public void registerFuture(ReactiveProcessor processor, String eventContextId, CompletableFuture<CoreEvent> response) {
    registry.put(new Pair<>(processor, eventContextId), response);
  }

  public void completeFuture(ReactiveProcessor processor, String eventContextId, CoreEvent value) {
    registry.remove(new Pair<>(processor, eventContextId)).complete(value);
  }

  public void completeFutureExceptionally(ReactiveProcessor processor, String eventContextId, Throwable ex) {
    registry.remove(new Pair<>(processor, eventContextId)).completeExceptionally(ex);
  }

}
