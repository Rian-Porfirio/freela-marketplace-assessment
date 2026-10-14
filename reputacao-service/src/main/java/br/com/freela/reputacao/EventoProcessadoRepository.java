package br.com.freela.reputacao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, UUID> {
    boolean existsByEventId(UUID eventId);
}
