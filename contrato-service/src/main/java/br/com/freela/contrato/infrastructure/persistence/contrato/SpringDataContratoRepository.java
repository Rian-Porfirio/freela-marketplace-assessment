package br.com.freela.contrato.infrastructure.persistence.contrato;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

interface SpringDataContratoRepository extends JpaRepository<ContratoJpaEntity, UUID> {
}
