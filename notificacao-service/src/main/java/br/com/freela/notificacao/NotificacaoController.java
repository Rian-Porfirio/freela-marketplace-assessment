package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/notificacoes")
public class NotificacaoController {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoController.class);
    private final NotificacaoRepository repository;

    NotificacaoController(NotificacaoRepository r) {
        this.repository = r;
    }

    @GetMapping
    public List<Notificacao> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.notificacao.listar correlationId={}", correlationId);
        return repository.findAll();
    }
}
