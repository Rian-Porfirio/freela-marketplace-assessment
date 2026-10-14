package br.com.freela.notificacao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
interface NotificacaoRepository extends JpaRepository<Notificacao, UUID> {
    boolean existsByEventId(String eventId);
}