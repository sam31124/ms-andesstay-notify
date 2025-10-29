package cl.duoc.andesstay.notify.consumer;

import cl.duoc.andesstay.notify.config.RabbitConsumerConfig;
import cl.duoc.andesstay.notify.model.MessageEnvelope;
import cl.duoc.andesstay.notify.model.NotificationLog;
import cl.duoc.andesstay.notify.service.AuditLogService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class HousekeepingConsumer {

    private static final Logger log = LoggerFactory.getLogger(HousekeepingConsumer.class);
    private final AuditLogService auditLogService;

    public HousekeepingConsumer(AuditLogService auditLogService) {
        this.auditLogService = auditLogService;
    }

    @RabbitListener(queues = RabbitConsumerConfig.QUEUE_HOUSEKEEPING, containerFactory = "rabbitListenerContainerFactory")
    public void receiveHousekeepingCommand(MessageEnvelope<Map<String, Object>> envelope,
                                           Channel channel,
                                           @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[HousekeepingConsumer] Recibiendo mensaje de '{}' [EventId: {}, Type: {}]",
                RabbitConsumerConfig.QUEUE_HOUSEKEEPING, envelope.getEventId(), envelope.getType());

        Map<String, Object> payload = envelope.getPayload();
        boolean simulateError = payload != null && Boolean.parseBoolean(String.valueOf(payload.getOrDefault("simulateError", "false")));
        String unidad = payload != null ? String.valueOf(payload.getOrDefault("unidad", "")) : "";

        // Regla de desvío a DLQ
        if (simulateError || unidad.isBlank() || unidad.toLowerCase().contains("fallid")) {
            String motivo = simulateError ? "Simulación de fallo en Housekeeping" : "Unidad no válida";
            log.error("[DLQ-REJECT] Mensaje no entregable en '{}'. Desviando a DLQ '{}' | EventId: {} | Motivo: {}",
                    RabbitConsumerConfig.QUEUE_HOUSEKEEPING, RabbitConsumerConfig.QUEUE_HOUSEKEEPING_DLQ, envelope.getEventId(), motivo);

            auditLogService.addLog(NotificationLog.create(
                    envelope.getEventId(),
                    envelope.getType(),
                    RabbitConsumerConfig.QUEUE_HOUSEKEEPING,
                    "DLQ_REDIRECTED",
                    envelope.getCorrelationId(),
                    envelope.getTraceId(),
                    "Ticket housekeeping rechazado hacia DLQ: " + motivo,
                    payload
            ));

            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // Generación de orden de trabajo
        Object resId = payload.getOrDefault("reservationId", "N/A");
        log.info("[HOUSEKEEPING TICKET SUCCESS] Orden de trabajo creada para unidad '{}' (Reserva #{}) | EventId: {}",
                unidad, resId, envelope.getEventId());

        auditLogService.addLog(NotificationLog.create(
                envelope.getEventId(),
                envelope.getType(),
                RabbitConsumerConfig.QUEUE_HOUSEKEEPING,
                "SUCCESS",
                envelope.getCorrelationId(),
                envelope.getTraceId(),
                "Ticket de preparación y limpieza generado para unidad: " + unidad,
                payload
        ));

        channel.basicAck(deliveryTag, false);
    }
}
