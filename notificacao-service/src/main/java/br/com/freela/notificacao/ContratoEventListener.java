package br.com.freela.notificacao;

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

@Component
@Slf4j
@RequiredArgsConstructor
public class ContratoEventListener {

    private final NotificacaoService notificacaoService;
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
            String eventType = json.get("eventType").asText();

            log.info("notificacao.evento.recebido eventType={} particao={} offset={}",
                    eventType, record.partition(), record.offset());
            try {
                notificacaoService.registrar(
                        eventId,
                        eventType,
                        contratoId,
                        UUID.fromString(json.get("clienteId").asText()),
                        UUID.fromString(json.get("freelancerId").asText()),
                        json.get("titulo").asText());
            } catch (DataIntegrityViolationException e) {
                log.warn("notificacao.evento.duplicado deteccao=constraint resultado=IGNORADO");
            }
        } finally {
            MDC.remove("correlationId");
            MDC.remove("eventId");
            MDC.remove("contratoId");
        }
    }
}
