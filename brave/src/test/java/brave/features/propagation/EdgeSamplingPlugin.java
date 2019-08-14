package brave.features.propagation;

import brave.propagation.TraceContext;

import static brave.propagation.ExtraFieldPropagation.FieldUpdater;

class EdgeSamplingPlugin extends SecondarySamplingPlugin {
  final boolean isEdge;

  EdgeSamplingPlugin(String localServiceName) {
    super("edge", "edge-sampled");
    this.isEdge = localServiceName.equals("edge");
  }

  /** This ensures the request is sampled locally when inside the edge service. */
  @Override boolean configSampled() {
    return isEdge;
  }

  /**
   * This implements ttl = 1 by propagating the sampling field "edge-sampled" from the edge service,
   * then redacting it at the next hop.
   */
  @Override protected FieldUpdater injectFieldUpdater(TraceContext context) {
    return (key, value) -> {
      if (samplingField.equals(key)) {
        return isEdge ? "1" : null;
      }
      return value;
    };
  }
}
