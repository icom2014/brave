package brave.features.propagation;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static brave.propagation.ExtraFieldPropagation.FieldUpdater;
import static brave.propagation.ExtraFieldPropagation.Plugin;
import static brave.propagation.ExtraFieldPropagation.get;

class SecondarySamplingPlugin extends Plugin {
  final String system;
  final String samplingField;

  SecondarySamplingPlugin(String system, String samplingField) {
    this.system = system;
    this.samplingField = samplingField;
  }

  @Override protected List<String> fieldNames() {
    return Arrays.asList(samplingField.toLowerCase(Locale.ROOT), "sampled");
  }

  /** Returns true when all requests should be sampled due to possibly dynamic configuration. */
  boolean configSampled() {
    return false;
  }

  /**
   * Returns true when this request should be sampled due to the value of the header, or a
   * combination of the header and possibly dynamic configuration.
   *
   * <p>Defaults to true when the sampling field exists.
   */
  boolean isSampled(String key, String value) {
    return samplingField.equals(key) && value != null;
  }

  /**
   * Regardless of the primary sampling decision, this will sample when {@link #configSampled()
   * configured} or when upstream set the {@link #samplingField sampling header} to "1".
   *
   * <p>As a side-effect, when sampled, this system is added to a comma-delimited extra field named
   * "sampled". This field will be available in-process, but it will be redacted (not propagated)
   * downstream. While this field is primarily internal state, it could be integrated with logging
   * contexts like MDC.
   */
  @Override
  protected FieldUpdater extractFieldUpdater(TraceContextOrSamplingFlags.Builder builder) {
    boolean[] sampled = {false};
    if (configSampled()) {
      builder.sampledLocal();
      sampled[0] = true;
    }
    return (key, value) -> {
      if (!sampled[0] && isSampled(key, value)) {
        builder.sampledLocal();
        sampled[0] = true;
      } else if ("sampled".equals(key) && sampled[0]) {
        return value != null ? system + "," + value : system;
      }
      return value;
    };
  }

  /** This makes sure that "sampled" field is not propagated out of process. */
  @Override protected FieldUpdater injectFieldUpdater(TraceContext context) {
    return (key, value) -> !"sampled".equals(key) ? value : null;
  }

  /** This adds a comma-delimited tag named "sampled" including "zipkin" if sampled remotely. */
  @Override protected FinishedSpanHandler finishedSpanHandler() {
    return new FinishedSpanHandler() {
      @Override public boolean handle(TraceContext context, MutableSpan span) {
        String sampled = get(context, "sampled");
        if (Boolean.TRUE.equals(context.sampled())) {
          sampled = sampled != null ? "zipkin," + sampled : "zipkin";
        }
        if (sampled != null) span.tag("sampled", sampled);
        return true;
      }
    };
  }
}
