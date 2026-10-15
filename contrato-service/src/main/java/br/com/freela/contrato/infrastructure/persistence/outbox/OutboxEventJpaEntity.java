package br.com.freela.contrato.infrastructure.persistence.outbox;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "outbox_event")
public class OutboxEventJpaEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long sequencia; // representa o número sequencial dos eventos registrados para ser publicado em ordem crescente pelo relay

    @Column(nullable = false, unique = true)
    private UUID eventId; // Só o ID do evento mesmo

    @Column(nullable = false)
    private UUID aggregateId; // que no caso é o contratoId (a chave pro kafka)

    @Column(nullable = false, length = 80)
    private String eventType; // Aqui é pra gente identificar qual é o tipo do evento (Ex:. ContratoConcluido)

    @Column(nullable = false, columnDefinition = "text")
    private String payload; // esse aqui é o conteúdo que será enviado para o tópico do contrato (o json inteiro)

    @Column(nullable = false)
    private Instant criadoEm; // informação sobre o momento em que o evento foi registrado na outbox

    private Instant publicadoEm; // informação sobre quando o evento foi publicado para o kafka

    private String correlationId;

    @Column(columnDefinition = "text")
    private String traceContext;

    public OutboxEventJpaEntity(UUID eventId, UUID aggregateId, String eventType,
                                String correlationId, String traceContext, String payload) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.correlationId = correlationId;
        this.traceContext = traceContext;
        this.payload = payload;
        this.criadoEm = Instant.now();
    }

    public void marcarComoPublicado() {
        this.publicadoEm = Instant.now();
    }
}