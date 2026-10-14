package br.com.freela.reputacao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReputacaoService {
    private final ReputacaoRepository repository;
    private final EventoProcessadoRepository eventoProcessadoRepository;

    @Transactional
    public void registrarContratoConcluido(UUID eventId, UUID contratoId, UUID freelancerId, BigDecimal valor) {
        log.info("reputacao.atualizacao.inicio contratoId={} freelancerId={} valor={}", contratoId, freelancerId, valor);
        if (!eventoProcessadoRepository.existsByEventId(eventId)) {
            eventoProcessadoRepository.saveAndFlush(new EventoProcessado(eventId));
            var reputacao = repository.findById(freelancerId)
                    .orElseGet(() -> new ReputacaoFreelancer(freelancerId));
            reputacao.registrarContrato(valor);
            repository.save(reputacao);
            log.info("reputacao.atualizacao.sucesso contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}", contratoId, freelancerId, reputacao.contratosConcluidos, reputacao.valorTotal);
        }
    }
}
