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
import brave.features.propagation.SecondaryConditionalSampling.Extra;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
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
 * compatible endpoint. At a high level, normal sampled Zipkin coexists with a customer support
 * system which needs 100% data for a subset of the network, based on request properties like a user
 * ID. Our goal to allow this coexistence without redundant overhead or reporting queues and without
 * sending incomplete data to Zipkin.
 *
 * <h2>Normal Zipkin vs Customer Support</h2>
 * <p>The support system in this example allows employees to conditionally get 100% data from a
 * limited area of the network, without flooding the normal Zipkin service, and without accidentally
 * sending partial data to the normal Zipkin service either. Regardless of whether customer service
 * asks for 100%, normal probabilistic (B3) sampling will occur. To review, Zipkin typically chooses
 * to keep a trace or not, and retains that same decision across the whole request (via B3 headers).
 * The customer service system not only uses different headers, but it triggers only at certain
 * nodes, and only on requests relevant to the customer support problem (ex same customer ID).
 *
 * <p>While Zipkin and Customer Support choose data differently, and use different backends, they
 * both share the same instrumentation and report data to the same Zipkin-compatible endpoint. This
 * assumes the endpoint is able to route data to Zipkin or Customer Support or both based on what it
 * sees in the Zipkin v2 json payloads. For example, if it sees a tag relating to customer support,
 * it knows to route the data there.
 *
 * <p>It is critical that these tools in no way change the api surface to instrumentation libraries
 * such as what's used by frameworks like Spring Boot. The fact that there are multiple systems
 * choosing data differently should not be noticeable by instrumentation, nor should it double
 * overhead or otherwise. Any re-instrumentation or double-overhead burden would burden engineers,
 * and represent an abstraction break.
 *
 * <h2>Implementing a sampling overlay in Brave</h2>
 * Brave assumes there is a primary, propagated sampling decision, captured in process as {@link
 * TraceContext#sampled()} at the beginning of the trace, and propagated downstream in B3 headers.
 * This simplifies the question of what to do about Customer Support, who can make a decision
 * anywhere.
 *
 * <p>Since Customer Support cannot use the primary sampling state used by B3, it can only use a
 * secondary (extra field). When Customer Support decides to record data, it must set {@link
 * TraceContext#sampledLocal()} so that this decision can override any unsampled decision in B3,
 * while not interfering with it. Finally, by setting {@link Tracing.Builder#alwaysReportSpans()},
 * data selected by Customer Support will report to the same span reporter as normal Zipkin.
 *
 * <p>Here's a simplified example of extra field handling from a sampling point of view:
 * <pre>{@code
 * class CustomerSupportSampler extends ExtraFieldPropagation.Customizer {
 *   @Override
 *   public FieldCustomizer extractCustomizer(TraceContextOrSamplingFlags.Builder builder) {
 *     return (fieldName, value) -> {
 *       // look at the extra field to see if it means we should boost the signal
 *       if (fieldName.equals("customer-support") && customerSupportWants(value)) {
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
 * <p>If we solely use {@link Tracing.Builder#alwaysReportSpans()} to get Customer Support's data
 * to the Zipkin endpoint, we could cause a problem. Since Customer Support's data is incomplete, it
 * could produce confusing results in the normal Zipkin backend. Moreover, it could add load to the
 * generic Zipkin backend. Similarly, it would be a problem to send all B3 data to Customer
 * support.
 *
 * <p>To make this point concrete, any of the below scenarios will now result in reported spans:
 * <ul>
 *   <li>B3 yes, but Customer Support no</li>
 *   <li>B3 no, but Customer Support yes</li>
 *   <li>Both yes</li>
 * </ul>
 *
 * With only two systems involved (B3 and Customer Support), this problem can be cleared up
 * unambiguously by looking at the flags {@link TraceContext#sampled()} (for B3) and
 * {@link TraceContext#sampledLocal()} (for customer Support). When a span is finished, we can add
 * a partioning tag to help the http collector decide if it should sent the data to either or both
 * backends.
 *
 * <p>Ex.
 * <pre>{@code
 * class AddSampledTag extends FinishedSpanHandler {
 *   // Propagate a data routing hint to the backend to ensure Customer Support doesn't end up
 *   // flooding Zipkin when B3 says no.
 *   @Override public boolean handle(TraceContext context, MutableSpan span) {
 *     boolean b3Sampled = Boolean.TRUE.equals(context.sampled()); // primary remote sampling
 *     boolean customerSupportSampled = context.sampledLocal(); // secondary overlay
 *     span.tag("sampled", b3Sampled && customerSupportSampled ? "zipkin,customer-support"
 *       : b3Sampled ? "b3" : "customerSupport");
 *     return true;
 *   }
 * }
 * }</pre>
 *
 * <h2>Summary</h2>
 * It is relatively easy to report more data based on a secondary decision made based on an extra
 * field. All you need to do is implement and configure {@link ExtraFieldPropagation.Customizer} to
 * {@link TraceContext#sampledLocal() sample local} based on the field value, and set the tracing
 * instance to {@link Tracing.Builder#alwaysReportSpans() always report spans}. If the spans already
 * contain data the backend needs to route to appropriate data stores, you are done. If it needs a
 * hint, you can add a {@link FinishedSpanHandler} to add a tag which ensures the backend consistently
 * and statelessly honors the sampling intent.
 */
public class SecondaryConditionalSamplingTest {
  List<zipkin2.Span> zipkin = new ArrayList<>();
  List<MutableSpan> edge = new ArrayList<>(), links = new ArrayList<>(), triage =
    new ArrayList<>();
  FinishedSpanHandler triageHandler = new FinishedSpanHandler() {
    @Override public boolean handle(TraceContext context, MutableSpan span) {
      return triage.add(span);
    }
  };

  SecondaryConditionalSampling secondaryConditionalSampling = SecondaryConditionalSampling.create()
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
    secondaryConditionalSampling.build(Tracing.newBuilder().spanReporter(zipkin::add));

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
    map.put("sampling", "edge:tps=1,ttl=3;links:sampled=1;triage:tps=5");

    Tracer tracer = tracing.tracer();
    Span span1 = tracer.nextSpan(extractor.extract(map)).name("span1").kind(Kind.SERVER).start();
    Span span2 = tracer.newChild(span1.context()).kind(Kind.CLIENT).name("span2").start();
    injector.inject(span2.context(), map);
    assertThat(map).containsEntry("sampling", "edge:ttl=3,sampled=1;links:sampled=1;triage:tps=5");

    // hop 1
    Span span3 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span4 = tracer.newChild(span3.context()).kind(Kind.CLIENT).name("span3").start();
    injector.inject(span4.context(), map);
    assertThat(map).containsEntry("sampling", "edge:sampled=1,ttl=2;links:sampled=1;triage:tps=5");

    // hop 2
    Span span5 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span6 = tracer.newChild(span5.context()).kind(Kind.CLIENT).name("span4").start();
    injector.inject(span6.context(), map);
    assertThat(map).containsEntry("sampling", "edge:sampled=1,ttl=1;links:sampled=1;triage:tps=5");

    // hop 3
    Span span7 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span8 = tracer.newChild(span7.context()).kind(Kind.CLIENT).name("span5").start();
    injector.inject(span8.context(), map);
    assertThat(map).containsEntry("sampling", "links:sampled=1;triage:tps=5");

    // dynamic configuration adds triage processing
    secondaryConditionalSampling.putSystem("triage", triageHandler);

    // hop 4, triage is now sampled
    Span span9 = tracer.nextSpan(extractor.extract(map)).kind(Kind.SERVER).start();
    Span span10 = tracer.newChild(span9.context()).kind(Kind.CLIENT).name("span6").start();
    injector.inject(span10.context(), map);
    assertThat(map).containsEntry("sampling", "links:sampled=1;triage:sampled=1");

    asList(span1, span2, span3, span4, span5, span6, span7, span8, span9, span10)
      .forEach(Span::finish);

    assertThat(zipkin).isEmpty();
    assertThat(edge).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(3);
    assertThat(links).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(5);
    assertThat(triage).filteredOn(s -> s.kind() == Kind.SERVER).hasSize(1);
  }

  @Test public void extract_samplesLocalWhenConfigured() {
    map.put("b3", "0");
    map.put("sampling", "links:sampled=0;triage:tps=5");

    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    map.put("b3", "0");
    map.put("sampling", "links:sampled=0;triage:sampled=1");

    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    map.put("b3", "0");
    map.put("sampling", "links:sampled=1;triage:tps=5");

    assertThat(extractor.extract(map).sampledLocal()).isTrue();
  }

  /** This shows an example of dynamic configuration */
  @Test public void dynamicConfiguration() {
    // base case: links is configured, triage is not. triage is in the headers, though!
    map.put("b3", "0");
    map.put("sampling", "links:sampled=1;triage:tps=5");

    assertThat(extractor.extract(map).sampledLocal()).isTrue();

    // dynamic configuration removes link processing
    secondaryConditionalSampling.removeSystem("links");
    assertThat(extractor.extract(map).sampledLocal()).isFalse();

    // dynamic configuration adds triage processing
    secondaryConditionalSampling.putSystem("triage", triageHandler);
    assertThat(extractor.extract(map).sampledLocal()).isTrue();

    tracing.tracer().nextSpan(extractor.extract(map)).start().finish();
    assertThat(zipkin).isEmpty();
    assertThat(edge).isEmpty();
    assertThat(links).isEmpty(); // no longer configured
    assertThat(triage).hasSize(1); // now configured
  }

  @Test public void extract_convertsConfiguredTpsToDecision() {
    map.put("b3", "0");
    map.put("sampling", "edge:tps=1,ttl=3;links:sampled=0;triage:tps=5");

    TraceContextOrSamplingFlags extracted = extractor.extract(map);
    Extra extra = (Extra) extracted.extra().get(0);
    assertThat(extra.systems)
      .containsEntry("edge", twoEntryMap("sampled", "1", "ttl", "3"))
      .containsEntry("links", singletonMap("sampled", "0"))
      .containsEntry("triage", singletonMap("tps", "5")); // triage is not configured
  }

  @Test public void extract_decrementsTtlWhenConfigured() {
    map.put("b3", "0");
    map.put("sampling", "edge:sampled=1,ttl=3;links:sampled=0,ttl=1;triage:tps=5");

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
      .containsEntry("sampling", "edge:tps=1,ttl=3;links:sampled=0;triage:tps=5");
  }

  static <K, V> Map<K, V> twoEntryMap(K key1, V value1, K key2, V value2) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(key1, value1);
    result.put(key2, value2);
    return result;
  }
}
