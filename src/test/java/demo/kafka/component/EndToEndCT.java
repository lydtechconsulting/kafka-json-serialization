package demo.kafka.component;

import java.util.List;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.util.TestEventData;
import dev.lydtech.component.framework.client.kafka.KafkaClient;
import dev.lydtech.component.framework.extension.TestContainersSetupExtension;
import dev.lydtech.component.framework.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static demo.kafka.util.TestEventData.INBOUND_DATA;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Slf4j
@ExtendWith(TestContainersSetupExtension.class)
public class EndToEndCT {

    private static final String GROUP_ID = "EndToEndCT";

    private Consumer consumer;

    @BeforeEach
    public void setup() {
        consumer = KafkaClient.getInstance().initConsumer(GROUP_ID, "demo-outbound-topic", 3L);
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    /**
     * Send in multiple keyed events and ensure an outbound event is emitted for each.
     */
    @Test
    public void testFlow() throws Exception {
        int totalMessages = 10;
        for (int i=0; i<totalMessages; i++) {
            DemoInboundKey testKey = TestEventData.buildDemoInboundKey(randomUUID(), randomUUID());
            DemoInboundPayload testPayload = TestEventData.buildDemoInboundPayload(randomUUID());
            KafkaClient.getInstance().sendMessage("demo-inbound-topic", JsonMapper.writeToJson(testKey), JsonMapper.writeToJson(testPayload));
        }
        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert("testFlow", consumer, totalMessages, 3);
        outboundEvents.stream().forEach(outboundEvent -> assertThat(outboundEvent.value(), containsString(INBOUND_DATA)));
    }
}
