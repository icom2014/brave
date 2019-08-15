/*
 * Copyright 2013-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.internal.handler;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FinishedSpanHandlers {
  public static FinishedSpanHandler compose(Collection<FinishedSpanHandler> finishedSpanHandlers) {
    if (finishedSpanHandlers.isEmpty()) return FinishedSpanHandler.NOOP;
    if (finishedSpanHandlers.size() == 1) return finishedSpanHandlers.iterator().next();
    int i = 0;
    boolean alwaysSampleLocal = false;
    List<FinishedSpanHandler> copy = new ArrayList<>();
    for (FinishedSpanHandler handler : finishedSpanHandlers) {
      if (handler.alwaysSampleLocal()) alwaysSampleLocal = true;
      if (handler instanceof CompositeFinishedSpanHandler) {
        copy.addAll(Arrays.asList(((CompositeFinishedSpanHandler) handler).handlers));
      } else {
        copy.add(handler);
      }
    }
    return new CompositeFinishedSpanHandler(copy, alwaysSampleLocal);
  }

  /**
   * When {@code noop}, this drops input spans by returning false. Otherwise, it logs exceptions
   * instead of raising an error, as the supplied handler could have bugs.
   */
  public static FinishedSpanHandler noopAware(FinishedSpanHandler handler, AtomicBoolean noop) {
    if (handler == FinishedSpanHandler.NOOP) return handler;
    return new NoopAwareFinishedSpanHandler(handler, noop);
  }

  static final class NoopAwareFinishedSpanHandler extends FinishedSpanHandler {
    final FinishedSpanHandler delegate;
    final AtomicBoolean noop;

    NoopAwareFinishedSpanHandler(FinishedSpanHandler delegate, AtomicBoolean noop) {
      if (delegate == null) throw new NullPointerException("delegate == null");
      this.delegate = delegate;
      this.noop = noop;
    }

    @Override public boolean handle(TraceContext context, MutableSpan span) {
      if (noop.get()) return false;
      try {
        return delegate.handle(context, span);
      } catch (RuntimeException e) {
        Platform.get().log("error accepting {0}", context, e);
        return false;
      }
    }

    @Override public boolean alwaysSampleLocal() {
      return delegate.alwaysSampleLocal();
    }

    @Override public String toString() {
      return delegate.toString();
    }
  }

  static final class CompositeFinishedSpanHandler extends FinishedSpanHandler {
    final FinishedSpanHandler[] handlers; // Array ensures no iterators are created at runtime
    final boolean alwaysSampleLocal;

    CompositeFinishedSpanHandler(List<FinishedSpanHandler> handlers, boolean alwaysSampleLocal) {
      this.handlers = handlers.toArray(new FinishedSpanHandler[0]);
      this.alwaysSampleLocal = alwaysSampleLocal;
    }

    @Override public boolean handle(TraceContext context, MutableSpan span) {
      for (FinishedSpanHandler handler : handlers) {
        if (!handler.handle(context, span)) return false;
      }
      return true;
    }

    @Override public boolean alwaysSampleLocal() {
      return alwaysSampleLocal;
    }

    @Override public String toString() {
      return "CompositeFinishedSpanHandler(" + Arrays.toString(handlers) + ")";
    }
  }
}
