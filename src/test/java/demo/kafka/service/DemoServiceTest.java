package demo.kafka.service;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundPayload;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.producer.KafkaDemoProducer;
import demo.kafka.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DemoServiceTest {

    private KafkaDemoProducer mockKafkaDemoProducer;
    private DemoService service;

    @BeforeEach
    public void setUp() {
        mockKafkaDemoProducer = mock(KafkaDemoProducer.class);
        service = new DemoService(mockKafkaDemoProducer);
    }

    /**
     * Ensure the Kafka producer is called to emit a message.
     */
    @Test
    public void testProcess() {
        DemoInboundKey testKey = TestEventData.buildDemoInboundKey(randomUUID(), randomUUID());
        DemoInboundPayload testPayload = TestEventData.buildDemoInboundPayload(randomUUID());

        service.process(testKey, testPayload);

        verify(mockKafkaDemoProducer, times(1)).sendMessage(any(DemoOutboundKey.class), any(DemoOutboundPayload.class));
    }
}
