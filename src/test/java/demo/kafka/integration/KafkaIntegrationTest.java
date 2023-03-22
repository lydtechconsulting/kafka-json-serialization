package demo.kafka.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.KafkaDemoConfiguration;
import demo.kafka.event.DemoInboundEvent;
import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.event.DemoOutboundKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static demo.kafka.util.TestEventData.buildDemoInboundEvent;
import static demo.kafka.util.TestEventData.buildDemoInboundKey;
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

        /**
         * Kafka beans for the integration test:  the test produces a DemoInboundEvent (with a DemoInboundKey) to be consumed
         * by the application and consumes a DemoOutboundEvent (with a DemoOutboundKey) emitted by the application.
         */

        @Bean
        public KafkaTemplate<DemoInboundKey, DemoInboundEvent> testKafkaTemplate(final ProducerFactory<DemoInboundKey, DemoInboundEvent> testProducerFactory) {
            return new KafkaTemplate<>(testProducerFactory);
        }

        @Bean(name = "testProducerFactory")
        public ProducerFactory<DemoInboundKey, DemoInboundEvent> testProducerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean(name = "testKafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<DemoOutboundKey, DemoOutboundEvent> testKafkaListenerContainerFactory(final ConsumerFactory<DemoOutboundKey, DemoOutboundEvent> testConsumerFactory) {
            final ConcurrentKafkaListenerContainerFactory<DemoOutboundKey, DemoOutboundEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(testConsumerFactory);
            return factory;
        }

        @Bean(name = "testConsumerFactory")
        public ConsumerFactory<DemoOutboundKey, DemoOutboundEvent> testConsumerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            config.put(JsonDeserializer.KEY_DEFAULT_TYPE, DemoOutboundKey.class.getCanonicalName());
            config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, DemoOutboundEvent.class.getCanonicalName());

            return new DefaultKafkaConsumerFactory<>(config);
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
        DemoInboundEvent inboundEvent = buildDemoInboundEvent(payloadId);
        kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundEvent).get();

        Awaitility.await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));

        assertThat(testReceiver.keyedMessages.size(), equalTo(1));
        assertThat(testReceiver.keyedMessages.get(0).getLeft().getId(), equalTo(keyPrimaryId));
        assertThat(testReceiver.keyedMessages.get(0).getRight().getOutboundData(), equalTo("Processed: " + inboundEvent.getInboundData()));
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
            DemoInboundEvent inboundEvent = buildDemoInboundEvent(payloadId);
            kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundEvent).get();
        }

        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(totalMessages));
    }
}
