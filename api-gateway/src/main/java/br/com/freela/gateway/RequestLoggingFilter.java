package br.com.freela.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {
    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = getCorrelationId(exchange);
        long inicio = System.currentTimeMillis();

        ServerHttpRequest request = exchange.getRequest().mutate().header(CORRELATION_ID_HEADER, correlationId).build();

        log.info("gateway.request.inicio correlationId={} method={} path={}", correlationId,
                request.getMethod(), request.getURI().getPath());

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(signal -> log.info(
                        "gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                        correlationId, request.getMethod(), request.getURI().getPath(),
                        exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, signal));
    }

    @Override
    public int getOrder() {
        return -100;
    }

    private String getCorrelationId(ServerWebExchange exchange) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(CORRELATION_ID_HEADER);

        if (correlationId == null || correlationId.isBlank()) {
            return UUID.randomUUID().toString();
        }

        return correlationId;
    }
}
