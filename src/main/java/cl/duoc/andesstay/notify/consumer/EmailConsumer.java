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
public class EmailConsumer {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);
    private final AuditLogService auditLogService;

    public EmailConsumer(AuditLogService auditLogService) {
        this.auditLogService = auditLogService;
    }

    @RabbitListener(queues = RabbitConsumerConfig.QUEUE_EMAIL, containerFactory = "rabbitListenerContainerFactory")
    public void receiveEmailCommand(MessageEnvelope<Map<String, Object>> envelope,
                                    Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[EmailConsumer] Recibiendo mensaje de '{}' [EventId: {}, Type: {}]",
                RabbitConsumerConfig.QUEUE_EMAIL, envelope.getEventId(), envelope.getType());

        Map<String, Object> payload = envelope.getPayload();
        boolean simulateError = payload != null && Boolean.parseBoolean(String.valueOf(payload.getOrDefault("simulateError", "false")));
        String email = payload != null ? String.valueOf(payload.getOrDefault("email", "")) : "";

        // Regla de desvío a DLQ: Si se solicita error intencional o datos corruptos
        if (simulateError || email.isBlank() || email.contains("error")) {
            String motivo = simulateError ? "Simulación de fallo programada" : "Email inválido o corrupto";
            log.error("[DLQ-REJECT] Mensaje no entregable en '{}'. Desviando automáticamente a DLQ '{}' | EventId: {} | Motivo: {}",
                    RabbitConsumerConfig.QUEUE_EMAIL, RabbitConsumerConfig.QUEUE_EMAIL_DLQ, envelope.getEventId(), motivo);

            auditLogService.addLog(NotificationLog.create(
                    envelope.getEventId(),
                    envelope.getType(),
                    RabbitConsumerConfig.QUEUE_EMAIL,
                    "DLQ_REDIRECTED",
                    envelope.getCorrelationId(),
                    envelope.getTraceId(),
                    "Rechazado y enviado a DLQ: " + motivo,
                    payload
            ));

            // NACK con requeue=false -> RabbitMQ lo envía al Dead Letter Exchange
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // Procesamiento exitoso
        String guest = String.valueOf(payload.getOrDefault("huesped", "Huésped"));
        Object resId = payload.getOrDefault("reservationId", "N/A");

        log.info("[EMAIL NOTIFY SUCCESS] Correo despachado exitosamente a '{}' ({}) para reserva #{} | EventId: {} | CorrelationId: {}",
                email, guest, resId, envelope.getEventId(), envelope.getCorrelationId());

        auditLogService.addLog(NotificationLog.create(
                envelope.getEventId(),
                envelope.getType(),
                RabbitConsumerConfig.QUEUE_EMAIL,
                "SUCCESS",
                envelope.getCorrelationId(),
                envelope.getTraceId(),
                "Email enviado a " + email + " para reserva #" + resId,
                payload
        ));

        // Confirmación manual ACK
        channel.basicAck(deliveryTag, false);
    }
}
