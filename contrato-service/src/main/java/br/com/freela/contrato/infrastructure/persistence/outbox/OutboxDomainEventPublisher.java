package br.com.freela.contrato.infrastructure.persistence.outbox;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.DomainEventPublisher;
import br.com.freela.contrato.infrastructure.filter.CorrelationIdFilter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class OutboxDomainEventPublisher implements DomainEventPublisher {

    private final OutboxEventJpaRepository outbox;
    private final ObjectMapper objectMapper;

    public OutboxDomainEventPublisher(OutboxEventJpaRepository outbox, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.objectMapper = objectMapper.copy()
                .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
    }

    @Override
    public void publicar(List<DomainEvent> eventos) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);

        for (DomainEvent evento : eventos) {
            MDC.put("eventId", evento.eventId().toString());
            try {
                ObjectNode json = objectMapper.valueToTree(evento);
                json.put("eventType", evento.eventType());
                json.put("correlationId", correlationId);

                String payload = objectMapper.writeValueAsString(json);

                outbox.save(new OutboxEventJpaEntity(
                        evento.eventId(),
                        evento.contratoId(),
                        evento.eventType(),
                        correlationId,
                        payload
                ));

                log.info("contrato.outbox.registrado eventType={}", evento.eventType());

            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Erro ao serializar evento da Outbox", e);
            } finally {
                MDC.remove("eventId");
            }
        }
    }
}