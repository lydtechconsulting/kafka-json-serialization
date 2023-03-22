package demo.kafka.producer;

import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaDemoProducer {
    @Autowired
    private final KafkaDemoProperties properties;

    @Autowired
    private final KafkaTemplate kafkaTemplate;

    public SendResult<DemoOutboundKey, DemoOutboundEvent> sendMessage(DemoOutboundKey key, DemoOutboundEvent event) {
        try {
            SendResult<DemoOutboundKey, DemoOutboundEvent> result = (SendResult) kafkaTemplate.send(properties.getOutboundTopic(), key, event).get();
            log.info("Emitted message - key: " + key + " id: " + event.getId() + " - payload: " + event.getOutboundData());
            return result;
        } catch (Exception e) {
            String message = "Error sending message to topic " + properties.getOutboundTopic();
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
