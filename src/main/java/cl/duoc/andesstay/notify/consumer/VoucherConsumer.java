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
public class VoucherConsumer {

    private static final Logger log = LoggerFactory.getLogger(VoucherConsumer.class);
    private final AuditLogService auditLogService;

    public VoucherConsumer(AuditLogService auditLogService) {
        this.auditLogService = auditLogService;
    }

    @RabbitListener(queues = RabbitConsumerConfig.QUEUE_VOUCHER, containerFactory = "rabbitListenerContainerFactory")
    public void receiveVoucherCommand(MessageEnvelope<Map<String, Object>> envelope,
                                      Channel channel,
                                      @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("[VoucherConsumer] Recibiendo mensaje de '{}' [EventId: {}, Type: {}]",
                RabbitConsumerConfig.QUEUE_VOUCHER, envelope.getEventId(), envelope.getType());

        Map<String, Object> payload = envelope.getPayload();
        boolean simulateError = payload != null && Boolean.parseBoolean(String.valueOf(payload.getOrDefault("simulateError", "false")));

        if (simulateError) {
            log.error("[DLQ-REJECT] Mensaje no entregable en '{}'. Desviando a DLQ '{}' | EventId: {}",
                    RabbitConsumerConfig.QUEUE_VOUCHER, RabbitConsumerConfig.QUEUE_VOUCHER_DLQ, envelope.getEventId());

            auditLogService.addLog(NotificationLog.create(
                    envelope.getEventId(),
                    envelope.getType(),
                    RabbitConsumerConfig.QUEUE_VOUCHER,
                    "DLQ_REDIRECTED",
                    envelope.getCorrelationId(),
                    envelope.getTraceId(),
                    "Generación de voucher rechazada hacia DLQ por error inducido",
                    payload
            ));

            channel.basicNack(deliveryTag, false, false);
            return;
        }

        Object resId = payload != null ? payload.getOrDefault("reservationId", "N/A") : "N/A";
        String guest = payload != null ? String.valueOf(payload.getOrDefault("huesped", "Cliente")) : "Cliente";

        log.info("[VOUCHER PDF SUCCESS] Documento PDF emitido con código VOUCH-{} para {} | EventId: {}",
                resId, guest, envelope.getEventId());

        auditLogService.addLog(NotificationLog.create(
                envelope.getEventId(),
                envelope.getType(),
                RabbitConsumerConfig.QUEUE_VOUCHER,
                "SUCCESS",
                envelope.getCorrelationId(),
                envelope.getTraceId(),
                "Voucher PDF generado: VOUCH-" + resId + " para " + guest,
                payload
        ));

        channel.basicAck(deliveryTag, false);
    }
}
