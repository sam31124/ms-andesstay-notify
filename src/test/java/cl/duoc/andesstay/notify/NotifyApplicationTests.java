package cl.duoc.andesstay.notify;

import cl.duoc.andesstay.notify.model.NotificationLog;
import cl.duoc.andesstay.notify.service.AuditLogService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class NotifyApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AuditLogService auditLogService;

    @MockBean
    private RabbitTemplate rabbitTemplate;

    @MockBean
    private ConnectionFactory connectionFactory;

    @Test
    void testAuditLogTracking() {
        auditLogService.clearLogs();
        assertEquals(0, auditLogService.getSuccessCount());
        assertEquals(0, auditLogService.getDlqCount());

        auditLogService.addLog(NotificationLog.create("e1", "TYPE_A", "q.cmd.email", "SUCCESS", "c1", "t1", "OK", null));
        auditLogService.addLog(NotificationLog.create("e2", "TYPE_B", "q.cmd.email.dlq", "DLQ_REDIRECTED", "c2", "t2", "Error", null));

        assertEquals(1, auditLogService.getSuccessCount());
        assertEquals(1, auditLogService.getDlqCount());
        assertEquals(2, auditLogService.getLogs().size());
    }

    @Test
    void testGetLogsEndpoint() throws Exception {
        mockMvc.perform(get("/api/notify/logs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.logs").isArray())
                .andExpect(jsonPath("$.successCount").isNumber())
                .andExpect(jsonPath("$.dlqCount").isNumber());
    }

    @Test
    void testSimulateSendEndpoint() throws Exception {
        String json = "{\"queue\":\"email\",\"simulateError\":true}";

        mockMvc.perform(post("/api/notify/simulate-send")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.routingKey").value("email.send"))
                .andExpect(jsonPath("$.eventId").exists());

        Mockito.verify(rabbitTemplate, Mockito.atLeastOnce())
                .convertAndSend(eq("cmd.direct"), eq("email.send"), any(Object.class));
    }
}
