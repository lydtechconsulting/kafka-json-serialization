package demo.kafka.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.properties.KafkaDemoProperties;
import demo.kafka.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaDemoProducerTest {

    private KafkaDemoProperties propertiesMock;
    private KafkaTemplate kafkaTemplateMock;
    private KafkaDemoProducer kafkaDemoProducer;

    @BeforeEach
    public void setUp() {
        propertiesMock = mock(KafkaDemoProperties.class);
        kafkaTemplateMock = mock(KafkaTemplate.class);
        kafkaDemoProducer = new KafkaDemoProducer(propertiesMock, kafkaTemplateMock);
    }

    /**
     * Ensure the Kafka client is called to emit a message.
     */
    @Test
    public void testSendMessage_Success() throws Exception {
        String key = "test-key";
        DemoOutboundEvent testEvent = TestEventData.buildDemoOutboundEvent(randomUUID().toString());
        String topic = "test-outbound-topic";

        when(propertiesMock.getOutboundTopic()).thenReturn(topic);
        CompletableFuture futureResult = mock(CompletableFuture.class);
        SendResult expectedSendResult = mock(SendResult.class);
        when(futureResult.get()).thenReturn(expectedSendResult);
        when(kafkaTemplateMock.send(topic, key, testEvent)).thenReturn(futureResult);

        SendResult result = kafkaDemoProducer.sendMessage(key, testEvent);

        verify(kafkaTemplateMock, times(1)).send(topic, key, testEvent);
        assertThat(result, equalTo(expectedSendResult));
    }

    /**
     * Ensure that an exception thrown on the send is cleanly handled.
     */
    @Test
    public void testSendMessage_ExceptionOnSend() throws Exception {
        String key = "test-key";
        DemoOutboundEvent testEvent = TestEventData.buildDemoOutboundEvent(randomUUID().toString());
        String topic = "test-outbound-topic";

        when(propertiesMock.getOutboundTopic()).thenReturn(topic);
        CompletableFuture futureResult = mock(CompletableFuture.class);
        when(kafkaTemplateMock.send(topic, key, testEvent)).thenReturn(futureResult);

        doThrow(new ExecutionException("Kafka send failure", new Exception("Failed"))).when(futureResult).get();

        Exception exception = assertThrows(RuntimeException.class, () -> {
                    kafkaDemoProducer.sendMessage(key, testEvent);
        });

        verify(kafkaTemplateMock, times(1)).send(topic, key, testEvent);
        assertThat(exception.getMessage(), equalTo("Error sending message to topic " + topic));
    }
}
