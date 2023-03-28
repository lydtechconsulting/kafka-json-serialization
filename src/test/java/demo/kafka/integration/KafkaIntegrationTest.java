package demo.kafka.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.KafkaDemoConfiguration;
import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.event.DemoOutboundKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static demo.kafka.util.TestEventData.buildDemoInboundKey;
import static demo.kafka.util.TestEventData.buildDemoInboundPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = { KafkaDemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = { "demo-inbound-topic", "demo-outbound-topic" })
public class KafkaIntegrationTest {

    final static String DEMO_INBOUND_TEST_TOPIC = "demo-inbound-topic";
    final static String DEMO_OUTBOUND_TEST_TOPIC = "demo-outbound-topic";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTestListener testReceiver;

    @Configuration
    static class TestConfig {

        /**
         * The test listener.
         */
        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);

        List<ImmutablePair<DemoOutboundKey, DemoOutboundEvent>> keyedMessages = new ArrayList<>();

        @KafkaListener(groupId = "KafkaIntegrationTest", topics = DEMO_OUTBOUND_TEST_TOPIC, autoStartup = "true")
        void receive(@Header(KafkaHeaders.RECEIVED_KEY) DemoOutboundKey key, @Payload final DemoOutboundEvent payload) {
            log.debug("KafkaTestListener - Received message: id: " + payload.getId() + " - outbound data: " + payload.getOutboundData() + " - key: " + key.getId());
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            keyedMessages.add(ImmutablePair.of(key, payload));
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
        testReceiver.counter.set(0);
        testReceiver.keyedMessages = new ArrayList<>();
    }

    /**
     * Send in a single event and ensure an outbound event is emitted.  Assert the outbound event is as expected.
     *
     * The key primaryId sent with the inbound event should match the key id received for the outbound event.
     */
    @Test
    public void testSuccess_SingleEvent() throws Exception {
        UUID keyPrimaryId = UUID.randomUUID();
        UUID keySecondaryId = UUID.randomUUID();
        DemoInboundKey inboundKey = buildDemoInboundKey(keyPrimaryId, keySecondaryId);

        UUID payloadId = UUID.randomUUID();
        DemoInboundPayload inboundPayload = buildDemoInboundPayload(payloadId);
        kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundPayload).get();

        Awaitility.await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));

        assertThat(testReceiver.keyedMessages.size(), equalTo(1));
        assertThat(testReceiver.keyedMessages.get(0).getLeft().getId(), equalTo(keyPrimaryId));
        assertThat(testReceiver.keyedMessages.get(0).getRight().getOutboundData(), equalTo("Processed: " + inboundPayload.getInboundData()));
    }

    /**
     * Send in multiple events and ensure an outbound event is emitted for each.
     */
    @Test
    public void testSuccess_MultipleEvents() throws Exception {
        int totalMessages = 10;
        for (int i=0; i<totalMessages; i++) {
            UUID keyPrimaryId = UUID.randomUUID();
            UUID keySecondaryId = UUID.randomUUID();
            DemoInboundKey inboundKey = buildDemoInboundKey(keyPrimaryId, keySecondaryId);

            UUID payloadId = UUID.randomUUID();
            DemoInboundPayload inboundPayload = buildDemoInboundPayload(payloadId);
            kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundPayload).get();
        }

        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(totalMessages));
    }
}
