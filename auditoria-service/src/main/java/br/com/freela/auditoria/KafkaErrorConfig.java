package br.com.freela.auditoria;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Slf4j
@Configuration
public class KafkaErrorConfig {

    private static final String SERVICO = "auditoria";
    private static final String TOPICO = "contrato.eventos";

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        var dlt = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(TOPICO + "." + SERVICO + ".DLT", record.partition()));

        var backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(8000);

        var handler = new DefaultErrorHandler((record, ex) -> {
            log.error("{}.evento.dlt topico={} particao={} offset={} motivo={}",
                    SERVICO, record.topic(), record.partition(), record.offset(), ex.toString());
            dlt.accept(record, ex);
        }, backOff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
