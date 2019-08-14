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
package brave.features.propagation;

import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracing;
import brave.features.propagation.SecondarySampling.Extra;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.ExtraFieldPropagation.FieldUpdater;
import brave.propagation.ExtraFieldPropagation.Plugin;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import zipkin2.reporter.Reporter;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This proves a concept needed for sites who have different tracing systems behind one Zipkin
 * compatible endpoint. At a high level, normal probabilistic B3 sampling co-exists with Edge
 * sampling (100% between the gateway and the first service behind it), and Triage sampling (100%
 * conditional on request properties like a user ID). Our goal to allow this coexistence without
 * redundant overhead or reporting queues and without sending irrelevant data to any of the
 * systems.
 *
 * <h2>Zipkin vs Edge vs Triage</h2>
 * In this example, there are three tracing systems: zipkin, edge and triage. The following points
 * describe elaborate both what they need and what they don't need.
 *
 * <h3>Zipkin</h3>
 * Zipkin is always on for the entire network, and if tracing occurs, it wants the entire trace from
 * the first request to the last on every node in the network. Zipkin is the primary owner of the
 * trace, and its state is in B3 headers, including the up-front decision on whether or not to
 * record data on this request. To review, [B3 propagates](https://github.com/openzipkin/b3-propagation)
 * through the process and across nodes, and the a sampling decision is trace-scoped: it never
 * changes from true to false or visa versa.c
 *
 * <h3>Edge</h3>
 * Edge tracing is traffic that starts at gateway of the architecture. Unlike Zipkin sampling, this
 * is 100% with the only condition being TTL, a simple TTL of one hop-down. 100% trace data only
 * including ingress and egress from the edge router can serve a lot of value as the teams can build
 * reliable statistics and flow charts. It is easy to implement as it is relatively stateless. For
 * example, the router always collects and the first node needs only to consume the instruction and
 * expire the sampling decision given to them. (not propagate it further).
 *
 * <h3>Triage</h3>
 * The triage system serves customer support goals. Like Edge, this is 100% data, but unlike Edge,
 * where data is collected is conditional on request properties such as the customer ID and the
 * services under investigation. In other words, the triage system is on-demand, and could cover a
 * small subset of the architecture starting at an arbitrary place. As such, triage is more advanced
 * that the other two systems.
 *
 * <h2>Trace forwarder</h2>
 * Each of the three systems have different capacities, retention rates and potentially different
 * billing implications. There should be no interference between these systems. For example, if
 * Zipkin is sampling 1% and while Triage is also on, still only the 1% sampled by B3 should be in
 * Zipkin. On the other hand, if Triage is off, it should not accidentally get 100% data from Edge.
 *
 * <p>The responsibility for this is a zipkin-compatible endpoint, which can route the same data to
 * one or more systems who want it. We'll call this the trace forwarder. Some examples are
 * [PitchFork](https://github.com/jeqo/zipkin-forwarder) and [Zipkin
 * Forwarder](https://github.com/jeqo/zipkin-forwarder). As trace data is completely out-of-band,
 * any inputs to help decide where to route the data must be in the Zipkin json. For example, if the
 * forwarder sees a tag relating to triage, it can build a forwarding rule based on this.
 *
 * <h2>The application is unaware of the other tracing systems</h2>
 * It is critical that these tools in no way change the api surface to instrumentation libraries
 * such as what's used by frameworks like Spring Boot. The fact that there are multiple systems
 * choosing data differently should not be noticeable by instrumentation. All of these systems use
 * the same trace and span IDs, which means log correlation is not affected. Sharing instrumentation
 * and reporting means we are not burdening the application with redundant overhead. It also means
 * we are not requiring engineering effort to re-instrument each time we add a system.
 *
 * <h2>Implementing a sampling overlay in Brave</h2>
 * Brave assumes there is a primary, propagated sampling decision, captured in process as {@link
 * TraceContext#sampled()} at the beginning of the trace, and propagated downstream in B3 headers.
 * This simplifies the question of what to do about Edge who make a constant decision, and Triage,
 * who make a conditional decision anywhere. Since they are not the primary deciders of the trace,
 * they cannot affect {@link TraceContext#sampled()}.
 *
 * <p>This implies Edge and Triage use secondary, extra fields, to propagate state about their
 * respective decisions. When Edge or Triage decides to record data, they must set {@link
 * TraceContext#sampledLocal()} so that recording occurs even if the primary decision is unsampled.
 * At startup, {@link Tracing.Builder#alwaysReportSpans()} ensures the trace forwarder gets data
 * regardless of the B3 decision.
 *
 * <p>Secondary fields and state management about them is the responsibility of {@link
 * ExtraFieldPropagation}. Here, you can add an extra field that corresponds to a header sent out of
 * process. You can also add a {@link Plugin} to ensure a relevant tag gets to the trace forwarder.
 *
 * TODO: continue to update
 *
 * <p>Here's a simplified example of extra field handling from a sampling point of view:
 * <pre>{@code
 * class TriageSampler extends ExtraFieldPropagation.Plugin {
 *   @Override
 *   public FieldUpdater extractFieldUpdater(TraceContextOrSamplingFlags.Builder builder) {
 *     return (fieldName, value) -> {
 *       // look at the extra field to see if it means we should boost the signal
 *       if (fieldName.equals("triage") && triageWants(value)) {
 *         builder.sampledLocal();
 *       }
 *       return value;
 *     };
 *   }
 * }
 * }</pre>
 *
 * <h2>Reporting data eventually to multiple systems</h2>
 * In normal Zipkin, sampling is implicit from a backend POV. If the collector gets data, it must
 * have been sampled. In code, when {@link TraceContext#sampled()} is true, timing data goes to the
 * {@link Tracing.Builder#spanReporter(Reporter) normal span reporter}. Most backends assume the
 * entire trace will be present and won't work well if it isn't.
 *
 * <p>If we solely use {@link Tracing.Builder#alwaysReportSpans()} to get Triage's data
 * to the Zipkin endpoint, we could cause a problem. Since Triage's data is incomplete, it could
 * produce confusing results in the normal Zipkin backend. Moreover, it could add load to the
 * generic Zipkin backend. Similarly, it would be a problem to send all B3 data to Customer
 * support.
 *
 * <p>To make this point concrete, any of the below scenarios will now result in reported spans:
 * <ul>
 *   <li>B3 yes, but Triage no</li>
 *   <li>B3 no, but Triage yes</li>
 *   <li>Both yes</li>
 * </ul>
 *
 * With only two systems involved (B3 and Triage), this problem can be cleared up
 * unambiguously by looking at the flags {@link TraceContext#sampled()} (for B3) and
 * {@link TraceContext#sampledLocal()} (for triage). When a span is finished, we can add
 * a partioning tag to help the http collector decide if it should sent the data to either or both
 * backends.
 *
 * <p>Ex.
 * <pre>{@code
 * class AddSampledTag extends FinishedSpanHandler {
 *   // Propagate a data routing hint to the backend to ensure Triage doesn't end up
 *   // flooding Zipkin when B3 says no.
 *   @Override public boolean handle(TraceContext context, MutableSpan span) {
 *     boolean b3Sampled = Boolean.TRUE.equals(context.sampled()); // primary remote sampling
 *     boolean triageSampled = context.sampledLocal(); // secondary overlay
 *     span.tag("sampled", b3Sampled && triageSampled ? "zipkin,triage"
 *       : b3Sampled ? "b3" : "triage");
 *     return true;
 *   }
 * }
 * }</pre>
 *
 * <h2>Summary</h2>
 * It is relatively easy to report more data based on a secondary decision made based on an extra
 * field. All you need to do is implement and configure {@link Plugin} to
 * {@link TraceContext#sampledLocal() sample local} based on the field value, and set the tracing
 * instance to {@link Tracing.Builder#alwaysReportSpans() always report spans}. If the spans already
 * contain data the backend needs to route to appropriate data stores, you are done. If it needs a
 * hint, you can add a {@link FinishedSpanHandler} to add a tag which ensures the backend consistently
 * and statelessly honors the sampling intent.
 */
public class SecondarySamplingTest {
  List<zipkin2.Span> zipkin = new ArrayList<>(), edge = new ArrayList<>(), triage =
    new ArrayList<>();

  Reporter<zipkin2.Span> traceForwarder = span -> {
    String systems = span.tags().get("systems");
    if (systems.contains("zipkin")) zipkin.add(span);
    if (systems.contains("edge")) edge.add(span);
    if (systems.contains("triage")) triage.add(span);
  };

  SecondarySampling secondarySampling = SecondarySampling.create()
    .putSystem("edge", new FinishedSpanHandler() {
      @Override public boolean handle(TraceContext context, MutableSpan span) {
        return edge.add(span);
      }
    })
    .putSystem("links", new FinishedSpanHandler() {
      @Override public boolean handle(TraceContext context, MutableSpan span) {
        return links.add(span);
      }
    });
  Tracing tracing =
    secondarySampling.build(Tracing.newBuilder().spanReporter(zipkin::add));

  TraceContext.Extractor<Map<String, String>> extractor = tracing.propagation().extractor(Map::get);
  TraceContext.Injector<Map<String, String>> injector = tracing.propagation().injector(Map::put);

  Map<String, String> map = new LinkedHashMap<>();

  @After public void close() {
    tracing.close();
  }

  /**
   * This shows when primary trace status is not sampled, we can send to handlers anyway.
   *
   * <p>At first, "triage" is not configured, so the tracer ignores it. Later, it is configured, so
   * starts receiving traces.
   */
  @Test public void integrationTest() {
    map.put("b3", "0");
    map.put("sampling", "edge:ttl=3;links;triage");

    Tracer tracer = tracing.tracer();
    Span span1 = tracer.nextSpan(extractor.extract(map)).name("span1").kind(Kind.SERVER).start();
    Span span2 = tracer.newChild(span1.context()).kind(Kind.CLIENT).name("span2").start();
    injector.inject(span2.context(), map);
    assertThat(map).containsEntry("sampling", "edge:ttl=3;links;triage");

    // hop 1
    Span span3 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span4 = tracer.newChild(span3.context()).kind(Kind.CLIENT).name("span3").start();
    injector.inject(span4.context(), map);
    assertThat(map).containsEntry("sampling", "edge:ttl=2;links;triage");

    // hop 2
    Span span5 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span6 = tracer.newChild(span5.context()).kind(Kind.CLIENT).name("span4").start();
    injector.inject(span6.context(), map);
    assertThat(map).containsEntry("sampling", "edge:ttl=1;links;triage");

    // hop 3
    Span span7 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span8 = tracer.newChild(span7.context()).kind(Kind.CLIENT).name("span5").start();
    injector.inject(span8.context(), map);
    assertThat(map).containsEntry("sampling", "links;triage");

    // dynamic configuration adds triage processing
    secondarySampling.putSystem("triage", triageHandler);

    // hop 4, triage is now sampled
    Span span9 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span10 = tracer.newChild(span9.context()).kind(Kind.CLIENT).name("span6").start();
    injector.inject(span10.context(), map);
    assertThat(map).containsEntry("sampling", "links;triage");

    asList(span1, span2, span3, span4, span5, span6, span7, span8, span9, span10)
      .forEach(Span::finish);

    assertThat(zipkin).isEmpty();
    assertThat(edge).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(3);
    assertThat(links).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(5);
    assertThat(triage).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(1);
  }

  @Test public void extract_samplesLocalWhenConfigured() {
    map.put("b3", "0");
    map.put("sampling", "links:sampled=0;triage");

    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    map.put("b3", "0");
    map.put("sampling", "links:sampled=0;triage");

    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    map.put("b3", "0");
    map.put("sampling", "links;triage");

    assertThat(extractor.extract(map).sampledLocal()).isTrue();
  }

  /** This shows an example of dynamic configuration */
  @Test public void dynamicConfiguration() {
    // base case: links is configured, triage is not. triage is in the headers, though!
    map.put("b3", "0");
    map.put("sampling", "links;triage");

    assertThat(extractor.extract(map).sampledLocal()).isTrue();

    // dynamic configuration removes link processing
    secondarySampling.removeSystem("links");
    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    // dynamic configuration adds triage processing
    secondarySampling.putSystem("triage", triageHandler);
    assertThat(extractor.extract(map).sampledLocal()).isTrue();

    tracing.tracer().nextSpan(extractor.extract(map)).start().finish();
    assertThat(zipkin).isEmpty();
    assertThat(edge).isEmpty();
    assertThat(links).isEmpty(); // no longer configured
    assertThat(triage).hasSize(1); // now configured
  }

  @Test public void extract_convertsConfiguredTpsToDecision() {
    map.put("b3", "0");
    map.put("sampling", "edge:tps=1,ttl=3;links:sampled=0;triage");

    TraceContextOrSamplingFlags extracted = extractor.extract(map);
    Extra extra = (Extra) extracted.extra().get(0);
    assertThat(extra.systems)
      .containsEntry("edge", twoEntryMap("sampled", "1", "ttl", "3"))
      .containsEntry("links", singletonMap("sampled", "0"))
      .containsEntry("triage", singletonMap("tps", "5")); // triage is not configured
  }

  @Test public void extract_decrementsTtlWhenConfigured() {
    map.put("b3", "0");
    map.put("sampling", "edge,ttl=3;links:sampled=0,ttl=1;triage");

    TraceContextOrSamplingFlags extracted = extractor.extract(map);
    Extra extra = (Extra) extracted.extra().get(0);
    assertThat(extra.systems)
      .containsEntry("edge", twoEntryMap("sampled", "1", "ttl", "2"))
      .doesNotContainKey("links")
      .containsEntry("triage", singletonMap("tps", "5"));
  }

  @Test public void injectWritesAllSystems() {
    Extra extra = new Extra();
    extra.systems.put("edge", twoEntryMap("tps", "1", "ttl", "3"));
    extra.systems.put("links", singletonMap("sampled", "0"));
    extra.systems.put("triage", singletonMap("tps", "5"));

    injector.inject(TraceContext.newBuilder()
      .traceId(1L).spanId(2L)
      .sampled(false)
      .extra(singletonList(extra))
      .build(), map);

    assertThat(map)
      .containsEntry("sampling", "edge:tps=1,ttl=3;links:sampled=0;triage");
  }

  static <K, V> Map<K, V> twoEntryMap(K key1, V value1, K key2, V value2) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(key1, value1);
    result.put(key2, value2);
    return result;
  }
}
