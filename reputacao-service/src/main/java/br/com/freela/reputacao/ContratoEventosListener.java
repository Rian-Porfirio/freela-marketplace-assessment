package br.com.freela.reputacao;

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

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContratoEventosListener {

    private final ReputacaoService reputacaoService;
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

            String tipo = json.get("eventType").asText();
            if (!tipo.equals("ContratoConcluido")) {
                log.info("reputacao.evento.ignorado eventType={} particao={} offset={}",
                        tipo, record.partition(), record.offset());
                return;
            }

            try {
                reputacaoService.registrarContratoConcluido(
                        eventId,
                        contratoId,
                        UUID.fromString(json.get("freelancerId").asText()),
                        json.get("valor").decimalValue());
            } catch (DataIntegrityViolationException e) {
                log.warn("reputacao.evento.duplicado deteccao=constraint resultado=IGNORADO");
            }
        } finally {
            MDC.remove("correlationId");
            MDC.remove("eventId");
            MDC.remove("contratoId");
        }
    }
}