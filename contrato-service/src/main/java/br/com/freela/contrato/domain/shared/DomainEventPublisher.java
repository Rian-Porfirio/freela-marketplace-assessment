package br.com.freela.contrato.domain.shared;

import java.util.List;

public interface DomainEventPublisher {
    void publicar(List<DomainEvent> eventos);
}
