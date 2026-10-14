package br.com.freela.reputacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/reputacoes")
public class ReputacaoController {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoController.class);
    private final ReputacaoRepository repository;

    ReputacaoController(ReputacaoRepository r) {
        this.repository = r;
    }

    @GetMapping
    public List<ReputacaoFreelancer> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.reputacao.listar correlationId={}", correlationId);
        return repository.findAll();
    }
}
