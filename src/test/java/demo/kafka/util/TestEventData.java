package demo.kafka.util;

import demo.kafka.event.DemoInboundEvent;
import demo.kafka.event.DemoOutboundEvent;

public class TestEventData {

    public static String INBOUND_DATA = "inbound event data";
    public static String OUTBOUND_DATA = "outbound event data";

    public static DemoInboundEvent buildDemoInboundEvent(String id) {
        return DemoInboundEvent.builder()
                .id(id)
                .inboundData(INBOUND_DATA)
                .build();
    }

    public static DemoOutboundEvent buildDemoOutboundEvent(String id) {
        return DemoOutboundEvent.builder()
                .id(id)
                .outboundData(OUTBOUND_DATA)
                .build();
    }
}
