package br.com.freela.notificacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes")
@Getter @Setter
class Notificacao {
    @Id
    UUID id;
    UUID contratoId;
    UUID destinatarioId;
    String tipo;
    @Column(length = 500)
    String mensagem;
    Instant criadaEm;
    String eventId;

    protected Notificacao() {
    }

    Notificacao(UUID contratoId, UUID destinatarioId, String tipo, String mensagem, String eventId) {
        this.id = UUID.randomUUID();
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
        this.eventId = eventId;
    }
}
