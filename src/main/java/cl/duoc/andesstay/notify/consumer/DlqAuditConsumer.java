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
public class DlqAuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqAuditConsumer.class);
    private final AuditLogService auditLogService;

    public DlqAuditConsumer(AuditLogService auditLogService) {
        this.auditLogService = auditLogService;
    }

    @RabbitListener(
            queues = {
                    RabbitConsumerConfig.QUEUE_EMAIL_DLQ,
                    RabbitConsumerConfig.QUEUE_HOUSEKEEPING_DLQ,
                    RabbitConsumerConfig.QUEUE_VOUCHER_DLQ
            },
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void processDlqMessage(MessageEnvelope<Map<String, Object>> envelope,
                                  Channel channel,
                                  @Header(AmqpHeaders.CONSUMER_QUEUE) String queue,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.warn("[DLQ-AUDIT RECEIVED] Mensaje en cola de fallos '{}' capturado para auditoría | EventId: {} | TraceId: {} | CorrelationId: {}",
                queue, envelope.getEventId(), envelope.getTraceId(), envelope.getCorrelationId());

        auditLogService.addLog(NotificationLog.create(
                envelope.getEventId(),
                envelope.getType(),
                queue,
                "DLQ_AUDITED",
                envelope.getCorrelationId(),
                envelope.getTraceId(),
                "Mensaje recuperado en Dead Letter Queue: " + queue,
                envelope.getPayload()
        ));

        // Confirmamos lectura del log en la DLQ
        channel.basicAck(deliveryTag, false);
    }
}
