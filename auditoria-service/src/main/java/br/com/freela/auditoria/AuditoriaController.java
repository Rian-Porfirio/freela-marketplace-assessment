package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);
    private final EventoAuditoriaRepository repository;

    AuditoriaController(EventoAuditoriaRepository r) {
        this.repository = r;
    }

    @GetMapping
    public List<EventoAuditoria> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.auditoria.listar correlationId={}", correlationId);
        return repository.findAll();
    }
}
