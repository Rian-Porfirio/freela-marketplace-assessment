package br.com.freela.reputacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
class EventoProcessado {
    @Id
    UUID id;

    @Column(nullable = false, unique = true)
    UUID eventId;

    Instant processadoEm;

    protected EventoProcessado() {
    }

    EventoProcessado(UUID eventId) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.processadoEm = Instant.now();
    }
}