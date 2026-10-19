package cl.duoc.andesstay.notify.controller;

import cl.duoc.andesstay.notify.config.RabbitConsumerConfig;
import cl.duoc.andesstay.notify.model.MessageEnvelope;
import cl.duoc.andesstay.notify.model.NotificationLog;
import cl.duoc.andesstay.notify.service.AuditLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/notify")
@CrossOrigin(origins = "*")
public class NotificationController {

    private static final Logger log = LoggerFactory.getLogger(NotificationController.class);

    private final AuditLogService auditLogService;
    private final RabbitTemplate localRabbitTemplate;
    private RabbitTemplate awsRabbitTemplate;

    public NotificationController(AuditLogService auditLogService, RabbitTemplate rabbitTemplate) {
        this.auditLogService = auditLogService;
        this.localRabbitTemplate = rabbitTemplate;
    }

    private synchronized RabbitTemplate getAwsRabbitTemplate() {
        if (this.awsRabbitTemplate == null) {
            CachingConnectionFactory awsFactory = new CachingConnectionFactory("54.162.30.246", 5672);
            awsFactory.setUsername("guest");
            awsFactory.setPassword("guest");
            awsFactory.setConnectionTimeout(4000);
            RabbitTemplate template = new RabbitTemplate(awsFactory);
            template.setMessageConverter(this.localRabbitTemplate.getMessageConverter());
            this.awsRabbitTemplate = template;
        }
        return this.awsRabbitTemplate;
    }

    @GetMapping("/logs")
    public ResponseEntity<?> getLogs() {
        return ResponseEntity.ok(Map.of(
                "successCount", auditLogService.getSuccessCount(),
                "dlqCount", auditLogService.getDlqCount(),
                "totalCount", auditLogService.getSuccessCount() + auditLogService.getDlqCount(),
                "logs", auditLogService.getLogs()
        ));
    }

    @GetMapping("/stats")
    public ResponseEntity<?> getStats() {
        return ResponseEntity.ok(Map.of(
                "successCount", auditLogService.getSuccessCount(),
                "dlqCount", auditLogService.getDlqCount(),
                "totalCount", auditLogService.getSuccessCount() + auditLogService.getDlqCount()
        ));
    }

    @PostMapping("/clear")
    public ResponseEntity<?> clearLogs() {
        auditLogService.clearLogs();
        return ResponseEntity.ok(Map.of("message", "Logs de mensajería limpiados con éxito"));
    }

    @PostMapping("/simulate-send")
    public ResponseEntity<?> simulateSend(@RequestBody(required = false) Map<String, Object> body) {
        if (body == null) body = new HashMap<>();

        String queueType = String.valueOf(body.getOrDefault("queue", "email"));
        boolean simulateError = Boolean.parseBoolean(String.valueOf(body.getOrDefault("simulateError", "false")));
        String target = String.valueOf(body.getOrDefault("target", "local")).toLowerCase();
        boolean isAws = "aws".equals(target) || "cloud".equals(target);

        Map<String, Object> payload = new HashMap<>(body);
        payload.putIfAbsent("huesped", "Juan Test Notificaciones");
        payload.putIfAbsent("reservationId", 101);
        payload.putIfAbsent("unidad", "Cabaña Vista Volcán");
        payload.putIfAbsent("email", simulateError ? "error.dlq@simulada.com" : "juan.test@andesstay.cl");
        payload.put("simulateError", simulateError);
        payload.put("targetBroker", isAws ? "AWS_EC2_CLOUD" : "LOCAL_DOCKER");

        String routingKey = switch (queueType.toLowerCase()) {
            case "housekeeping" -> "housekeeping.ticket";
            case "voucher" -> "voucher.gen";
            default -> "email.send";
        };

        String dlqQueueName = "q.cmd." + queueType.toLowerCase() + ".dlq";

        MessageEnvelope<Map<String, Object>> envelope = MessageEnvelope.of(
                simulateError ? "TEST_SIMULATED_ERROR" : "TEST_NOTIFICATION_EVENT",
                payload
        );

        if (isAws) {
            log.info("[SIMULATION-AWS] Despachando a RabbitMQ Cloud en AWS (54.162.30.246) | QueueType: {} | Error: {}", queueType, simulateError);
            RabbitTemplate awsTemplate = getAwsRabbitTemplate();

            if (simulateError) {
                // En AWS despachamos al Dead Letter Exchange directamente a la DLQ correspondiente
                awsTemplate.convertAndSend(RabbitConsumerConfig.EXCHANGE_DEAD_LETTER, dlqQueueName, envelope);
                auditLogService.addLog(NotificationLog.create(
                        envelope.getEventId(),
                        envelope.getType(),
                        "q.cmd." + queueType.toLowerCase() + " (AWS)",
                        "DLQ_REDIRECTED",
                        envelope.getCorrelationId(),
                        envelope.getTraceId(),
                        "☁️ [AWS Cloud EC2: 54.162.30.246] Fallo simulado: despachado a Dead Letter Queue '" + dlqQueueName + "'",
                        payload
                ));
            } else {
                // Despacho normal a Exchange Direct
                awsTemplate.convertAndSend(RabbitConsumerConfig.EXCHANGE_DIRECT, routingKey, envelope);
                auditLogService.addLog(NotificationLog.create(
                        envelope.getEventId(),
                        envelope.getType(),
                        "q.cmd." + queueType.toLowerCase() + " (AWS)",
                        "SUCCESS",
                        envelope.getCorrelationId(),
                        envelope.getTraceId(),
                        "☁️ [AWS Cloud EC2: 54.162.30.246] Mensaje despachado exitosamente a exchange '" + RabbitConsumerConfig.EXCHANGE_DIRECT + "'",
                        payload
                ));
            }

            return ResponseEntity.ok(Map.of(
                    "message", simulateError ? "☁️ Mensaje de prueba despachado a la DLQ en AWS Cloud" : "☁️ Mensaje de prueba despachado a AWS Cloud",
                    "broker", "AWS Cloud (54.162.30.246:5672)",
                    "exchange", simulateError ? RabbitConsumerConfig.EXCHANGE_DEAD_LETTER : RabbitConsumerConfig.EXCHANGE_DIRECT,
                    "routingKey", simulateError ? dlqQueueName : routingKey,
                    "eventId", envelope.getEventId(),
                    "correlationId", envelope.getCorrelationId()
            ));
        } else {
            // Despacho al broker Local
            localRabbitTemplate.convertAndSend(RabbitConsumerConfig.EXCHANGE_DIRECT, routingKey, envelope);

            return ResponseEntity.ok(Map.of(
                    "message", simulateError ? "💻 Mensaje de prueba para DLQ despachado a Local" : "💻 Mensaje de prueba exitoso despachado a Local",
                    "broker", "Local Docker (localhost:5672)",
                    "exchange", RabbitConsumerConfig.EXCHANGE_DIRECT,
                    "routingKey", routingKey,
                    "eventId", envelope.getEventId(),
                    "correlationId", envelope.getCorrelationId()
            ));
        }
    }
}
