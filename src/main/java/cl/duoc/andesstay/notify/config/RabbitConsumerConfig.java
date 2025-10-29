package cl.duoc.andesstay.notify.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitConsumerConfig {

    public static final String EXCHANGE_DIRECT = "cmd.direct";
    public static final String EXCHANGE_DLX = "cmd.dead.dlx";

    public static final String QUEUE_EMAIL = "q.cmd.email";
    public static final String QUEUE_HOUSEKEEPING = "q.cmd.housekeeping";
    public static final String QUEUE_VOUCHER = "q.cmd.voucher";

    public static final String QUEUE_EMAIL_DLQ = "q.cmd.email.dlq";
    public static final String QUEUE_HOUSEKEEPING_DLQ = "q.cmd.housekeeping.dlq";
    public static final String QUEUE_VOUCHER_DLQ = "q.cmd.voucher.dlq";

    public static final String ROUTING_KEY_EMAIL_DLQ = "email.dlq";
    public static final String ROUTING_KEY_HOUSEKEEPING_DLQ = "housekeeping.dlq";
    public static final String ROUTING_KEY_VOUCHER_DLQ = "voucher.dlq";

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter jsonMessageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false); // Rechazos con basicNack van directo al DLX
        return factory;
    }

    // Exchanges
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(EXCHANGE_DLX, true, false);
    }

    // DLQs
    @Bean
    public Queue emailDlq() {
        return QueueBuilder.durable(QUEUE_EMAIL_DLQ).build();
    }

    @Bean
    public Queue housekeepingDlq() {
        return QueueBuilder.durable(QUEUE_HOUSEKEEPING_DLQ).build();
    }

    @Bean
    public Queue voucherDlq() {
        return QueueBuilder.durable(QUEUE_VOUCHER_DLQ).build();
    }

    @Bean
    public Binding bindingEmailDlq() {
        return BindingBuilder.bind(emailDlq()).to(deadLetterExchange()).with(ROUTING_KEY_EMAIL_DLQ);
    }

    @Bean
    public Binding bindingHousekeepingDlq() {
        return BindingBuilder.bind(housekeepingDlq()).to(deadLetterExchange()).with(ROUTING_KEY_HOUSEKEEPING_DLQ);
    }

    @Bean
    public Binding bindingVoucherDlq() {
        return BindingBuilder.bind(voucherDlq()).to(deadLetterExchange()).with(ROUTING_KEY_VOUCHER_DLQ);
    }

    // Colas con DLX
    @Bean
    public Queue emailQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", EXCHANGE_DLX);
        args.put("x-dead-letter-routing-key", ROUTING_KEY_EMAIL_DLQ);
        return QueueBuilder.durable(QUEUE_EMAIL).withArguments(args).build();
    }

    @Bean
    public Queue housekeepingQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", EXCHANGE_DLX);
        args.put("x-dead-letter-routing-key", ROUTING_KEY_HOUSEKEEPING_DLQ);
        return QueueBuilder.durable(QUEUE_HOUSEKEEPING).withArguments(args).build();
    }

    @Bean
    public Queue voucherQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", EXCHANGE_DLX);
        args.put("x-dead-letter-routing-key", ROUTING_KEY_VOUCHER_DLQ);
        return QueueBuilder.durable(QUEUE_VOUCHER).withArguments(args).build();
    }
}
