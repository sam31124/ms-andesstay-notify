package cl.duoc.andesstay.notify.model;

import java.time.Instant;
import java.util.UUID;

public class NotificationLog {

    private String id;
    private String eventId;
    private String type;
    private String queue;
    private String status; // SUCCESS, DLQ_REDIRECTED, DLQ_AUDITED
    private String correlationId;
    private String traceId;
    private String timestamp;
    private String details;
    private Object payload;

    public NotificationLog() {
    }

    public NotificationLog(String id, String eventId, String type, String queue, String status,
                           String correlationId, String traceId, String timestamp, String details, Object payload) {
        this.id = id;
        this.eventId = eventId;
        this.type = type;
        this.queue = queue;
        this.status = status;
        this.correlationId = correlationId;
        this.traceId = traceId;
        this.timestamp = timestamp;
        this.details = details;
        this.payload = payload;
    }

    public static NotificationLog create(String eventId, String type, String queue, String status,
                                         String correlationId, String traceId, String details, Object payload) {
        return new NotificationLog(
                UUID.randomUUID().toString(),
                eventId,
                type,
                queue,
                status,
                correlationId,
                traceId,
                Instant.now().toString(),
                details,
                payload
        );
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }

    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }
}
