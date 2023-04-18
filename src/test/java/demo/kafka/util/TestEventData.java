package demo.kafka.util;

import java.util.UUID;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundPayload;
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

    public static DemoInboundPayload buildDemoInboundPayload(UUID id) {
        return DemoInboundPayload.builder()
                .id(id)
                .inboundData(INBOUND_DATA)
                .build();
    }

    public static DemoOutboundKey buildDemoOutboundKey(UUID id) {
        return DemoOutboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoOutboundPayload buildDemoOutboundEvent(UUID id) {
        return DemoOutboundPayload.builder()
                .id(id)
                .outboundData(OUTBOUND_DATA)
                .build();
    }
}
