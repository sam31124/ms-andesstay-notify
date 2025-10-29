package cl.duoc.andesstay.notify.controller;

import cl.duoc.andesstay.notify.config.RabbitConsumerConfig;
import cl.duoc.andesstay.notify.model.MessageEnvelope;
import cl.duoc.andesstay.notify.service.AuditLogService;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/notify")
@CrossOrigin(origins = "*")
public class NotificationController {

    private final AuditLogService auditLogService;
    private final RabbitTemplate rabbitTemplate;

    public NotificationController(AuditLogService auditLogService, RabbitTemplate rabbitTemplate) {
        this.auditLogService = auditLogService;
        this.rabbitTemplate = rabbitTemplate;
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

        Map<String, Object> payload = new HashMap<>(body);
        payload.putIfAbsent("huesped", "Juan Test Notificaciones");
        payload.putIfAbsent("reservationId", 101);
        payload.putIfAbsent("unidad", "Cabaña Vista Volcán");
        payload.putIfAbsent("email", simulateError ? "error.dlq@simulada.com" : "juan.test@andesstay.cl");
        payload.put("simulateError", simulateError);

        String routingKey = switch (queueType.toLowerCase()) {
            case "housekeeping" -> "housekeeping.ticket";
            case "voucher" -> "voucher.gen";
            default -> "email.send";
        };

        MessageEnvelope<Map<String, Object>> envelope = MessageEnvelope.of(
                simulateError ? "TEST_SIMULATED_ERROR" : "TEST_NOTIFICATION_EVENT",
                payload
        );

        rabbitTemplate.convertAndSend(RabbitConsumerConfig.EXCHANGE_DIRECT, routingKey, envelope);

        return ResponseEntity.ok(Map.of(
                "message", simulateError ? "Mensaje de prueba para DLQ despachado" : "Mensaje de prueba exitoso despachado",
                "exchange", RabbitConsumerConfig.EXCHANGE_DIRECT,
                "routingKey", routingKey,
                "eventId", envelope.getEventId(),
                "correlationId", envelope.getCorrelationId()
        ));
    }
}
