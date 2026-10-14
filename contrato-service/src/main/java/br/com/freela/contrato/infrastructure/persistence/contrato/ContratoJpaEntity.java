package br.com.freela.contrato.infrastructure.persistence.contrato;

import br.com.freela.contrato.domain.model.StatusContrato;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "contratos")
public class ContratoJpaEntity {
    @Id
    private UUID id;
    @Column(nullable = false)
    private UUID clienteId;
    @Column(nullable = false)
    private UUID freelancerId;
    @Column(nullable = false, length = 160)
    private String titulo;
    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal valor;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 40)
    private StatusContrato status;
    @Column(nullable = false)
    private Instant criadoEm;
}
