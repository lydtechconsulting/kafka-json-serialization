package demo.kafka.service;

import demo.kafka.event.DemoInboundEvent;
import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.producer.KafkaDemoProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoService {

    @Autowired
    private final KafkaDemoProducer kafkaDemoProducer;

    public void process(DemoInboundKey key, DemoInboundEvent event) {
        DemoOutboundEvent outboundEvent = DemoOutboundEvent.builder()
                .id(event.getId())
                .outboundData("Processed: " + event.getInboundData())
                .build();
        // Set the id of the outbound key to the primaryId of the inbound key.
        DemoOutboundKey outboundKey = DemoOutboundKey.builder()
                .id(key.getPrimaryId())
                .build();
        kafkaDemoProducer.sendMessage(outboundKey, outboundEvent);
    }
}
