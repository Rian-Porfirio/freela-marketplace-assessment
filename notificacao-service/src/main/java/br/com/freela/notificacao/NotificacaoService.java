package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class NotificacaoService {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);
    private final NotificacaoRepository repository;

    public NotificacaoService(NotificacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void registrar(UUID eventId, String eventType, UUID contratoId,
                          UUID clienteId, UUID freelancerId, String titulo) {
        log.info("notificacao.processamento.inicio eventId={} eventType={} contratoId={}",
                eventId, eventType, contratoId);

        if (repository.existsByEventId(eventId.toString())) {
            log.info("notificacao.evento.duplicado eventId={} contratoId={} resultado=IGNORADO",
                    eventId, contratoId);
            return;
        }

        List<Notificacao> notificacoes = montar(eventId, eventType, contratoId, clienteId, freelancerId, titulo);
        repository.saveAllAndFlush(notificacoes);

        for (Notificacao n : notificacoes) {
            log.info("notificacao.registrada eventId={} contratoId={} destinatarioId={} tipo={} resultado=REGISTRADA",
                    eventId, contratoId, n.getDestinatarioId(), n.getTipo());
        }
    }

    private List<Notificacao> montar(UUID eventId, String tipo, UUID contratoId,
                                     UUID clienteId, UUID freelancerId, String titulo) {
        String id = eventId.toString();
        return switch (tipo) {
            case "ContratoCriado" -> List.of(
                    new Notificacao(contratoId, freelancerId, tipo,
                            "Um novo contrato foi criado para você: " + titulo, id));
            case "EntregaRegistrada" -> List.of(
                    new Notificacao(contratoId, clienteId, tipo,
                            "A entrega do contrato foi registrada: " + titulo, id));
            case "ContratoConcluido" -> List.of(
                    new Notificacao(contratoId, clienteId, tipo,
                            "Seu contrato foi concluído: " + titulo, id),
                    new Notificacao(contratoId, freelancerId, tipo,
                            "O contrato foi concluído: " + titulo, id));
            case "ContratoCancelado" -> List.of(
                    new Notificacao(contratoId, freelancerId, tipo,
                            "O contrato foi cancelado: " + titulo, id));
            default -> {
                log.warn("notificacao.evento.desconhecido eventType={} eventId={}", tipo, eventId);
                yield List.of();
            }
        };
    }
}