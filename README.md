# Microservicio de Notificaciones y Consumo Desacoplado - AndesStay

**Asignatura:** DSY1107 - Desarrollo Cloud Native I  
**Evaluación:** Evaluación Parcial N°3 y N°4  
**Equipo:** Amaro Ulloa, Lucas Salazar, Samuel Urzúa  
**Puerto:** `8083`  

---

## 1. Descripción
Microservicio consumidor de mensajería asíncrona implementado en **Java 21** y **Spring Boot 3.3.4**. Se encarga de procesar los eventos de dominio de reservas hotelera de forma no bloqueante, implementando confirmación manual explícita y desvío de errores hacia **Dead Letter Queues (DLQ)**.

---

## 2. Topología de Colas y Consumidores

| Cola de Negocio | Dead Letter Queue (DLQ) | Consumidor Dedicado | Routing Key |
| :--- | :--- | :--- | :--- |
| `q.cmd.email` | `q.cmd.email.dlq` | `EmailConsumer.java` | `reservation.created` / `email.send` |
| `q.cmd.housekeeping` | `q.cmd.housekeeping.dlq` | `HousekeepingConsumer.java` | `reservation.status.changed` / `housekeeping.ticket` |
| `q.cmd.voucher` | `q.cmd.voucher.dlq` | `VoucherConsumer.java` | `reservation.confirmed` / `voucher.gen` |

---

## 3. Manejo de Confirmación Manual (ACK / NACK)

El microservicio utiliza `AcknowledgeMode.MANUAL` gestionado explícitamente en el canal AMQP:

* **Procesamiento Exitoso:**
  ```java
  channel.basicAck(deliveryTag, false);
  ```
* **Fallo de Negocio o Validación (Desvío a DLQ):**
  ```java
  // Sin reencolado (requeue=false) -> derivado de inmediato a cmd.dead.dlx
  channel.basicNack(deliveryTag, false, false);
  ```

---

## 4. Auditoría Forense (`DlqAuditConsumer`)
El microservicio incluye el componente `DlqAuditConsumer`, el cual escucha las colas `.dlq` y persiste los mensajes muertos con su `traceId`, `eventId`, motivo del rechazo y timestamp, exponiéndolos mediante la API REST:
* **GET `/api/notify/logs`**: Listado de logs de auditoría en memoria para supervisión en el Frontend.
* **POST `/api/notify/simulate-dlq?queue=email`**: Endpoint de simulación para inducir fallos y verificar el desvío a DLQ en vivo.

---

## 5. Tests Automatizados
```bash
.\mvnw.cmd test
```
* **Resultado:** `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0` (`BUILD SUCCESS`).
