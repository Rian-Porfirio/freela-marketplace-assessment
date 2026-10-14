package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "reputacoes")
@Getter
@Setter
class ReputacaoFreelancer {
    @Id
    UUID freelancerId;
    int contratosConcluidos;
    BigDecimal valorTotal = BigDecimal.ZERO;

    protected ReputacaoFreelancer() {
    }

    ReputacaoFreelancer(UUID id) {
        this.freelancerId = id;
    }

    void registrarContrato(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }
}
