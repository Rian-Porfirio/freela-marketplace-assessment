package br.com.freela.auditoria;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContratoEventListener {

    private final AuditoriaService auditoriaService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "contrato.eventos")
    public void receber(ConsumerRecord<String, String> record) throws JsonProcessingException {
        JsonNode json = objectMapper.readTree(record.value());
        String correlationId = json.hasNonNull("correlationId") ? json.get("correlationId").asText() : null;
        try {
            if (correlationId != null) {
                MDC.put("correlationId", correlationId);
            }
            UUID eventId = UUID.fromString(json.get("eventId").asText());
            UUID contratoId = UUID.fromString(json.get("contratoId").asText());
            MDC.put("eventId", eventId.toString());
            MDC.put("contratoId", contratoId.toString());

            log.info("auditoria.evento.recebido particao={} offset={}", record.partition(), record.offset());
            try {
                auditoriaService.registrar(
                        eventId,
                        contratoId,
                        json.get("eventType").asText(),
                        Instant.parse(json.get("occurredAt").asText()),
                        correlationId,
                        record.value());
            } catch (DataIntegrityViolationException e) {
                log.warn("auditoria.registro.duplicado deteccao=constraint resultado=IGNORADO");
            }
        } finally {
            MDC.remove("correlationId");
            MDC.remove("eventId");
            MDC.remove("contratoId");
        }
    }
}