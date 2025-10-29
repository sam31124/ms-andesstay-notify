package cl.duoc.andesstay.notify.service;

import cl.duoc.andesstay.notify.model.NotificationLog;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AuditLogService {

    private final ConcurrentLinkedDeque<NotificationLog> logs = new ConcurrentLinkedDeque<>();
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger dlqCount = new AtomicInteger(0);
    private static final int MAX_LOGS = 100;

    public void addLog(NotificationLog log) {
        if ("SUCCESS".equalsIgnoreCase(log.getStatus())) {
            successCount.incrementAndGet();
        } else if ("DLQ_REDIRECTED".equalsIgnoreCase(log.getStatus()) || "DLQ_AUDITED".equalsIgnoreCase(log.getStatus())) {
            dlqCount.incrementAndGet();
        }

        logs.addFirst(log);
        while (logs.size() > MAX_LOGS) {
            logs.removeLast();
        }
    }

    public List<NotificationLog> getLogs() {
        return Collections.unmodifiableList(new ArrayList<>(logs));
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getDlqCount() {
        return dlqCount.get();
    }

    public void clearLogs() {
        logs.clear();
        successCount.set(0);
        dlqCount.set(0);
    }
}
