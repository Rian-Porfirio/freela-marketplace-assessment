package br.com.freela.gateway;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import org.springframework.stereotype.Component;

@Component
public class IgnorarSpansDeInfraHandler extends SpanHandler {

    private static final String[] CHAVES = {"http.url", "url.full", "http.path", "url.path", "uri"};
    private static final String[] IGNORAR = {"/eureka", "/actuator"};

    @Override
    public boolean end(TraceContext context, MutableSpan span, Cause cause) {
        for (String chave : CHAVES) {
            String valor = span.tag(chave);
            if (valor != null) {
                for (String ignorar : IGNORAR) {
                    if (valor.contains(ignorar)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}