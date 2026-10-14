package br.com.freela.contrato.infrastructure.persistence.messaging;

import br.com.freela.contrato.infrastructure.configuration.KafkaConfig;
import br.com.freela.contrato.infrastructure.persistence.outbox.OutboxEventJpaEntity;
import br.com.freela.contrato.infrastructure.persistence.outbox.OutboxEventJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventJpaRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publicarEventos() {
        for (OutboxEventJpaEntity evento : outbox.findTop50ByPublicadoEmIsNullOrderBySequenciaAsc()) {
            if (evento.getCorrelationId() != null) {
                MDC.put("correlationId", evento.getCorrelationId());
            }
            MDC.put("eventId", evento.getEventId().toString());
            MDC.put("contratoId", evento.getAggregateId().toString());
            try {
                var record = new ProducerRecord<String, String>(
                        KafkaConfig.TOPICO_CONTRATO_EVENTOS,
                        evento.getAggregateId().toString(),
                        evento.getPayload());
                if (evento.getCorrelationId() != null) {
                    record.headers().add("correlationId",
                            evento.getCorrelationId().getBytes(StandardCharsets.UTF_8));
                }
                kafka.send(record).get(5, TimeUnit.SECONDS);
                evento.marcarComoPublicado();
                log.info("contrato.outbox.publicado eventType={}", evento.getEventType());
            } catch (Exception e) {
                log.error("contrato.outbox.falha eventType={} motivo={}", evento.getEventType(), e.toString());
                break;
            } finally {
                MDC.remove("correlationId");
                MDC.remove("eventId");
                MDC.remove("contratoId");
            }
        }
    }
}
