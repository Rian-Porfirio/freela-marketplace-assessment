package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class AuditoriaService {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
    private final EventoAuditoriaRepository repository;

    public AuditoriaService(EventoAuditoriaRepository r) {
        this.repository = r;
    }

    @Transactional
    public void registrar(UUID eventId, UUID aggregateId, String eventType, Instant occurredAt, String correlationId, String payload) {
        if ("FalhaTeste".equals(eventType)) {
            throw new IllegalStateException("falha de teste");
        }

        log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}", eventId, aggregateId, eventType, correlationId);

        if (!repository.existsByEventId(eventId)) {
            var e = repository.save(new EventoAuditoria(eventId, aggregateId, eventType, correlationId, payload));
            log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}", e.id, eventId, aggregateId, eventType);
        }
    }
}
