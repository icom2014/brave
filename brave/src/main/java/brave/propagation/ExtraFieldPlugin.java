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
package brave.propagation;

import brave.Tracing;
import brave.handler.FinishedSpanHandler;
import brave.internal.handler.FinishedSpanHandlers;
import brave.propagation.ExtraFieldPropagation.FieldUpdater;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Propagation plugins are self-contained and support advanced integration patterns such as metrics
 * aggregation or sampling overlays.
 */
public abstract class ExtraFieldPlugin {
  /**
   * Returns a list of unique lower-case field names used by this plugin.
   *
   * <p>The value of this is only read when building {@link ExtraFieldPropagation} and during
   * {@link #toString()}.
   */
  protected abstract List<String> fieldNames();

  /**
   * This is called once during {@link TraceContext.Extractor#extract(Object)}, allowing you to
   * decorate the primary trace state with secondary data from extra fields.
   *
   * <p>One use case is to override the sampling decision based on an alternate header. If the
   * decision is intended to be permanent for the rest of the trace, use {@link
   * TraceContextOrSamplingFlags.Builder#sampled(boolean)}, which affects the primary (usually B3)
   * headers. Otherwise, you can set {@link TraceContextOrSamplingFlags.Builder#sampledLocal()} to
   * make an overlaid decision.
   *
   * <p>If you are making an overlaid decision, you should either implement {@link
   * #finishedSpanHandler()} here, or set {@link Tracing.Builder#alwaysReportSpans()} if your
   * backend can tolerate inconsistent data.
   *
   * <p>The resulting field updater will called for each {@link #fieldNames() field name} in the
   * order they were configured, not in the order headers were received.
   */
  protected FieldUpdater extractFieldUpdater(TraceContextOrSamplingFlags.Builder builder) {
    return FieldUpdater.NOOP;
  }

  /**
   * This allows you to customize or prevent extra fields from going to the next host.
   *
   * <p>The resulting field updater will be called for each {@link #fieldNames() field name} in
   * the order they were configured.
   */
  protected FieldUpdater injectFieldUpdater(TraceContext context) {
    return FieldUpdater.NOOP;
  }

  /** This allows you to set tags based on extra fields, most commonly for log correlation. */
  // Intentionally protected to prevent accidental registration with Tracing.Builder
  protected FinishedSpanHandler finishedSpanHandler() {
    return FinishedSpanHandler.NOOP;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "{" + fieldNames() + "}";
  }

  public static ExtraFieldPlugin compose(ExtraFieldPlugin[] extraFieldPlugins) {
    if (extraFieldPlugins.length == 1) return extraFieldPlugins[0];
    return new CompositeExtraFieldPlugin(extraFieldPlugins);
  }

  static final class CompositeExtraFieldPlugin extends ExtraFieldPlugin {
    final ExtraFieldPlugin[] plugins; // Array ensures no iterators are created at runtime
    final List<String> fieldNames;
    final FinishedSpanHandler finishedSpanHandler;

    CompositeExtraFieldPlugin(ExtraFieldPlugin[] plugins) {
      this.plugins = plugins;
      Set<String> fieldNames = new LinkedHashSet<>();
      List<FinishedSpanHandler> finishedSpanHandlers = new ArrayList<>();
      for (ExtraFieldPlugin plugin : plugins) {
        fieldNames.addAll(plugin.fieldNames());
        if (plugin.finishedSpanHandler() != FinishedSpanHandler.NOOP) {
          finishedSpanHandlers.add(plugin.finishedSpanHandler());
        }
      }
      this.fieldNames = Collections.unmodifiableList(new ArrayList<>(fieldNames));
      this.finishedSpanHandler = finishedSpanHandlers.isEmpty() ? FinishedSpanHandler.NOOP
        : finishedSpanHandlers.size() == 1 ? finishedSpanHandlers.get(0)
          : FinishedSpanHandlers.compose(finishedSpanHandlers);
    }

    @Override protected List<String> fieldNames() {
      return fieldNames;
    }

    @Override
    protected FieldUpdater extractFieldUpdater(TraceContextOrSamplingFlags.Builder builder) {
      FieldUpdater[] fieldUpdaters = fieldUpdatersArray();
      for (int i = 0, length = plugins.length; i < length; i++) {
        fieldUpdaters[i] = plugins[i].extractFieldUpdater(builder);
      }
      return compositeFieldUpdater(fieldUpdaters);
    }

    @Override protected FieldUpdater injectFieldUpdater(TraceContext context) {
      FieldUpdater[] fieldUpdaters = fieldUpdatersArray();
      for (int i = 0, length = plugins.length; i < length; i++) {
        fieldUpdaters[i] = plugins[i].injectFieldUpdater(context);
      }
      return compositeFieldUpdater(fieldUpdaters);
    }

    @Override protected FinishedSpanHandler finishedSpanHandler() {
      return finishedSpanHandler;
    }

    @Override public String toString() {
      return "CompositeExtraFieldPlugin(" + Arrays.toString(plugins) + ")";
    }

    static FieldUpdater compositeFieldUpdater(ExtraFieldPropagation.FieldUpdater[] fieldUpdaters) {
      return (name, value) -> {
        for (FieldUpdater fieldUpdater : fieldUpdaters) {
          value = fieldUpdater.update(name, value);
        }
        return value;
      };
    }

    @SuppressWarnings("ThreadLocalUsage") // intentional: instances may have different plugin counts
    final ThreadLocal<Object[]> FIELD_UPDATERS = new ThreadLocal<>();

    FieldUpdater[] fieldUpdatersArray() {
      Object[] fieldUpdatersArray = FIELD_UPDATERS.get();
      if (fieldUpdatersArray == null) {
        fieldUpdatersArray = new FieldUpdater[plugins.length];
        FIELD_UPDATERS.set(fieldUpdatersArray);
      }
      return (FieldUpdater[]) fieldUpdatersArray;
    }
  }
}
