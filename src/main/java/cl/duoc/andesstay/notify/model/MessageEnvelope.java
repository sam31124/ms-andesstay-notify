package cl.duoc.andesstay.notify.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class MessageEnvelope<T> implements Serializable {

    private String eventId;
    private String type;
    private String timestamp;
    private String traceId;
    private String correlationId;
    private T payload;

    public MessageEnvelope() {
    }

    public MessageEnvelope(String eventId, String type, String timestamp, String traceId, String correlationId, T payload) {
        this.eventId = eventId;
        this.type = type;
        this.timestamp = timestamp;
        this.traceId = traceId;
        this.correlationId = correlationId;
        this.payload = payload;
    }

    public static <T> MessageEnvelope<T> of(String type, T payload) {
        String corrId = UUID.randomUUID().toString();
        return new MessageEnvelope<>(
                UUID.randomUUID().toString(),
                type,
                Instant.now().toString(),
                "trace-" + corrId.substring(0, 8),
                corrId,
                payload
        );
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public T getPayload() { return payload; }
    public void setPayload(T payload) { this.payload = payload; }
}
