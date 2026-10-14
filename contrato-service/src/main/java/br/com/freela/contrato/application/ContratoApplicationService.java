package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ContratoApplicationService {
    private final ContratoRepository repository;
    private final DomainEventPublisher eventos;

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        MDC.put("contratoId", contrato.id().toString());
        try {
            log.info("contrato.dominio.criado status={} domainEvents={}",
                    contrato.status(), contrato.domainEvents().size());

            Contrato salvo = repository.salvar(contrato);
            eventos.publicar(contrato.pullDomainEvents());
            log.info("contrato.criacao.sucesso clienteId={} freelancerId={} status={}",
                    salvo.clienteId(), salvo.freelancerId(), salvo.status());
            return salvo;
        } finally {
            MDC.remove("contratoId");
        }
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        log.info("contrato.entrega.inicio contratoId={}", id);
        Contrato contrato = carregar(id);
        contrato.registrarEntrega();
        Contrato salvo = repository.salvar(contrato);
        eventos.publicar(contrato.pullDomainEvents());
        log.info("contrato.entrega.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato concluir(UUID id) {
        log.info("contrato.conclusao.inicio contratoId={}", id);
        Contrato contrato = carregar(id);
        contrato.concluir();
        Contrato salvo = repository.salvar(contrato);
        eventos.publicar(contrato.pullDomainEvents());
        log.info("contrato.conclusao.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }


    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));

        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();

        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    private Contrato carregar(UUID id) {
        return repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
    }
}
