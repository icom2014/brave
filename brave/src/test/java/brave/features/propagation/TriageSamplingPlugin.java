package brave.features.propagation;

import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static brave.propagation.ExtraFieldPropagation.FieldUpdater;

class TriageSamplingPlugin extends SecondarySamplingPlugin {
  final Map<String, String> customerToStartOfTrace = new LinkedHashMap<>();
  final String localServiceName;

  TriageSamplingPlugin(String localServiceName) {
    super("triage", "triage-ttl");
    this.localServiceName = localServiceName;
  }

  @Override protected List<String> fieldNames() {
    ArrayList<String> result = new ArrayList<>(super.fieldNames());
    result.add(0, "customer-id"); // look at customer-id first
    return result;
  }

  boolean shouldStartInvestigation(String customer) {
    return localServiceName.equals(customerToStartOfTrace.get(customer));
  }

  /** This update implements the decrement side of TTL */
  @Override
  protected FieldUpdater extractFieldUpdater(TraceContextOrSamplingFlags.Builder builder) {
    FieldUpdater defaultImpl = super.extractFieldUpdater(builder);
    boolean[] startTrace = {false};
    return (name, value) -> {
      if (name.equals("customer-id")) {
        if (value != null && shouldStartInvestigation(value)) {
          startTrace[0] = true;
        }
      } else if (name.equals(samplingField)) {
        if (startTrace[0]) {
          value = "5"; // start TTL at 5
        } else if (value != null) { // If there's a TTL, decrement or expire it
          value = value.equals("1") ? null : Integer.toString(Integer.parseInt(value) - 1);
        }
      }
      return defaultImpl.update(name, value);
    };
  }
}
