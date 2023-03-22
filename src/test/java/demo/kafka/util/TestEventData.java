package demo.kafka.util;

import java.util.UUID;

import demo.kafka.event.DemoInboundEvent;
import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoOutboundEvent;
import demo.kafka.event.DemoOutboundKey;

public class TestEventData {

    public static String INBOUND_DATA = "inbound event data";
    public static String OUTBOUND_DATA = "outbound event data";

    public static DemoInboundKey buildDemoInboundKey(UUID primaryId, UUID secondaryId) {
        return DemoInboundKey.builder()
                .primaryId(primaryId)
                .secondaryId(secondaryId)
                .build();
    }

    public static DemoInboundEvent buildDemoInboundEvent(UUID id) {
        return DemoInboundEvent.builder()
                .id(id)
                .inboundData(INBOUND_DATA)
                .build();
    }

    public static DemoOutboundKey buildDemoOutboundKey(UUID id) {
        return DemoOutboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoOutboundEvent buildDemoOutboundEvent(UUID id) {
        return DemoOutboundEvent.builder()
                .id(id)
                .outboundData(OUTBOUND_DATA)
                .build();
    }
}
