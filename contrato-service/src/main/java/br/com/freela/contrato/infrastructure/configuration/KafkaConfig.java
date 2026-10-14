package br.com.freela.contrato.infrastructure.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaConfig {

    public static final String TOPICO_CONTRATO_EVENTOS = "contrato.eventos";

    @Bean
    NewTopic topic() {
        return TopicBuilder.name(TOPICO_CONTRATO_EVENTOS)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    NewTopic dltAuditoria() {
        return TopicBuilder.name("contrato.eventos.auditoria.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic dltNotificacao() {
        return TopicBuilder.name("contrato.eventos.notificacao.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic dltReputacao() {
        return TopicBuilder.name("contrato.eventos.reputacao.DLT").partitions(3).replicas(1).build();
    }
}
