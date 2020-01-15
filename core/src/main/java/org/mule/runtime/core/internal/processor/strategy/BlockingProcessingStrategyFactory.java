/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import static org.mule.runtime.core.api.rx.Exceptions.unwrap;
import static org.mule.runtime.core.api.rx.Exceptions.wrapFatal;
import static org.slf4j.LoggerFactory.getLogger;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Mono.subscriberContext;

import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.construct.FlowConstruct;
import org.mule.runtime.core.api.construct.Pipeline;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType;
import org.mule.runtime.core.api.processor.Sink;
import org.mule.runtime.core.api.processor.strategy.ProcessingStrategy;
import org.mule.runtime.core.api.processor.strategy.ProcessingStrategyFactory;
import org.mule.runtime.core.internal.exception.MessagingException;
import org.mule.runtime.core.internal.processor.BlockingExecutionContext;
import org.mule.runtime.core.internal.rx.FluxSinkRecorder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

/**
 * Processing strategy that processes the {@link Pipeline} in the caller thread and does not schedule the processing of any
 * {@link Processor} in a different thread pool regardless of their {@link ProcessingType}.
 * <p/>
 * When individual {@link Processor}'s execute non-blocking operations using additional threads internally (e.g. an outbound HTTP
 * request) the {@link Pipeline} will block until the operation response is available before continuing processing in the same
 * thread.
 */
public class BlockingProcessingStrategyFactory implements ProcessingStrategyFactory {

  private static final Logger LOGGER = getLogger(BlockingProcessingStrategy.class);

  public static final ProcessingStrategy BLOCKING_PROCESSING_STRATEGY_INSTANCE = new BlockingProcessingStrategy();

  @Override
  public ProcessingStrategy create(MuleContext muleContext, String schedulersNamePrefix) {
    return BLOCKING_PROCESSING_STRATEGY_INSTANCE;
  }

  @Override
  public Class<? extends ProcessingStrategy> getProcessingStrategyType() {
    return BLOCKING_PROCESSING_STRATEGY_INSTANCE.getClass();
  }

  private static class BlockingProcessingStrategy extends AbstractProcessingStrategy {

    @Override
    public boolean isSynchronous() {
      return true;
    }

    @Override
    public Sink createSink(FlowConstruct flowConstruct, ReactiveProcessor pipeline) {
      return new StreamPerEventSink(pipeline, event -> {
      });
    }

    @Override
    public ReactiveProcessor onProcessor(ReactiveProcessor processor) {
      return publisher -> subscriberContext()
          .flatMapMany(ctx -> {
            final FluxSinkRecorder<CoreEvent> processSink = processorAsSink(processor, ctx);

            return from(publisher)
                .doOnComplete(() -> processSink.complete())
                .onErrorContinue((t, e) -> processSink.error(t))
                .handle((event, sink) -> {
                  try {
                    final CompletableFuture<CoreEvent> response = new CompletableFuture<>();
                    BlockingExecutionContext.from(event).registerFuture(processor, event.getContext().getId(), response);
                    processSink.next(event);

                    CoreEvent result = response.get();
                    if (result != null) {
                      sink.next(result);
                    }
                  } catch (ExecutionException throwable) {
                    sink.error(wrapFatal(unwrap(throwable.getCause())));
                  } catch (Throwable throwable) {
                    sink.error(wrapFatal(unwrap(throwable)));
                  }
                });
          });
    }

  }

  public static FluxSinkRecorder<CoreEvent> processorAsSink(ReactiveProcessor processor, Context ctx) {
    final FluxSinkRecorder<CoreEvent> processSink = new FluxSinkRecorder<>();
    Flux.create(processSink)
        .transform(processor)
        .doOnNext(event -> BlockingExecutionContext.from(event)
            .completeFuture(processor, event.getContext().getId(), event))
        .onErrorContinue((t, failingEvent) -> {
          CoreEvent event;
          if (t instanceof MessagingException) {
            MessagingException me = (MessagingException) t;
            event = me.getEvent();
          } else {
            event = (CoreEvent) failingEvent;
          }
          BlockingExecutionContext.from(event)
              .completeFutureExceptionally(processor, event.getContext().getId(), t);
        })
        .subscriberContext(ctx)
        .subscribe(e -> {
        },
                   t -> LOGGER
                       .error("Exception reached blocking PS subscriber for processor '" + processor.toString() + "'",
                              t));
    return processSink;
  }

}
