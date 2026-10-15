# Especificação das mensagens

Contrato de comunicação entre os microsserviços do Freela Marketplace.

## 1. Visão geral

| Item | Valor |
|---|---|
| Tópico | `contrato.eventos` (3 partições, 1 réplica) |
| Produtor | contrato-service (via Transactional Outbox) |
| Consumidores | notificacao-service, reputacao-service, auditoria-service (grupos de consumo distintos) |
| Chave de publicação | `contratoId` (mesma chave → mesma partição → ordem preservada por contrato) |
| Formato | JSON, UTF-8, um objeto plano por mensagem |
| Entrega | At-least-once; consumidores idempotentes pelo `eventId` |
| Headers Kafka | `correlationId` (cópia do campo do corpo) e contexto de trace (`traceparent`) |

## 2. Estrutura da mensagem

A estrutura é a mesma nos quatro eventos. Só muda o valor de `eventType`.

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `eventId` | UUID | sim | Identificador único do evento. Gerado uma vez, na criação do evento, e nunca alterado em reenvios |
| `eventType` | string | sim | Nome do evento: `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido` ou `ContratoCancelado` |
| `occurredAt` | instante ISO-8601 (UTC) | sim | Momento em que o fato ocorreu |
| `correlationId` | string | sim | Identificador da operação, gerado pelo API Gateway |
| `contratoId` | UUID | sim | Contrato ao qual o evento se refere |
| `clienteId` | UUID | sim | Cliente do contrato |
| `freelancerId` | UUID | sim | Freelancer do contrato |
| `titulo` | string | sim | Título do contrato |
| `valor` | número decimal | sim | Valor do contrato |

O `correlationId` é obrigatório porque o Gateway o gera sempre que o cliente não envia um. O `contrato-service` assume que toda requisição passa pelo Gateway.

## 3. Eventos

### 3.1 ContratoCriado

| Item | Valor |
|---|---|
| Nome do evento | `ContratoCriado` |
| Quando ocorre | Um contrato é criado (status ATIVO) |
| Tópico | `contrato.eventos` |
| Serviço produtor | contrato-service |
| Consumidores que agem | notificacao-service (notifica o freelancer), auditoria-service (registra o evento) |
| Consumidores que ignoram | reputacao-service |
| Chave | `contratoId` |
| Campos obrigatórios | `eventId`, `eventType`, `occurredAt`, `correlationId`, `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor` |

```json
{
  "eventId": "a5d18ad4-66d2-4a50-8e9f-f93b3531c50f",
  "eventType": "ContratoCriado",
  "occurredAt": "2026-10-14T23:36:03.328Z",
  "correlationId": "demo-001",
  "contratoId": "bcc1bf58-f7f7-41e4-a0e6-50b13871f19a",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "11111111-1111-1111-1111-111111111113",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

### 3.2 EntregaRegistrada

| Item | Valor |
|---|---|
| Nome do evento | `EntregaRegistrada` |
| Quando ocorre | O freelancer registra a entrega (ATIVO → ENTREGA_REGISTRADA) |
| Tópico | `contrato.eventos` |
| Serviço produtor | contrato-service |
| Consumidores que agem | notificacao-service (notifica o cliente), auditoria-service |
| Consumidores que ignoram | reputacao-service |
| Chave | `contratoId` |
| Campos obrigatórios | `eventId`, `eventType`, `occurredAt`, `correlationId`, `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor` |

```json
{
  "eventId": "70ccfbcd-bedf-4e31-bca1-ea9d6c32c621",
  "eventType": "EntregaRegistrada",
  "occurredAt": "2026-10-14T23:36:05.531Z",
  "correlationId": "demo-002",
  "contratoId": "bcc1bf58-f7f7-41e4-a0e6-50b13871f19a",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "11111111-1111-1111-1111-111111111113",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

### 3.3 ContratoConcluido

| Item | Valor |
|---|---|
| Nome do evento | `ContratoConcluido` |
| Quando ocorre | O contrato é concluído (ENTREGA_REGISTRADA → CONCLUIDO) |
| Tópico | `contrato.eventos` |
| Serviço produtor | contrato-service |
| Consumidores que agem | notificacao-service (notifica cliente e freelancer), reputacao-service (incrementa contratos concluídos e valor total do freelancer), auditoria-service |
| Chave | `contratoId` |
| Campos obrigatórios | `eventId`, `eventType`, `occurredAt`, `correlationId`, `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor` |

A reputação usa `freelancerId` e `valor`; ela é o consumidor em que a duplicidade causaria dano (contador somado duas vezes).

```json
{
  "eventId": "120d6ffc-954e-4184-987e-4b64bbc1cb15",
  "eventType": "ContratoConcluido",
  "occurredAt": "2026-10-14T23:36:07.183Z",
  "correlationId": "demo-003",
  "contratoId": "bcc1bf58-f7f7-41e4-a0e6-50b13871f19a",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "11111111-1111-1111-1111-111111111113",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

### 3.4 ContratoCancelado

| Item | Valor |
|---|---|
| Nome do evento | `ContratoCancelado` |
| Quando ocorre | O contrato é cancelado (a regra atual só impede o cancelamento de contratos concluídos) |
| Tópico | `contrato.eventos` |
| Serviço produtor | contrato-service |
| Consumidores que agem | notificacao-service (notifica o freelancer), auditoria-service |
| Consumidores que ignoram | reputacao-service |
| Chave | `contratoId` |
| Campos obrigatórios | `eventId`, `eventType`, `occurredAt`, `correlationId`, `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor` |

Evento definido no domínio e nesta especificação; ainda não há endpoint HTTP que acione o cancelamento.

```json
{
  "eventId": "4f6c9d2a-1b7e-4c35-9a10-2d8e5b7f3a61",
  "eventType": "ContratoCancelado",
  "occurredAt": "2026-10-14T23:40:00.000Z",
  "correlationId": "demo-004",
  "contratoId": "bcc1bf58-f7f7-41e4-a0e6-50b13871f19a",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "11111111-1111-1111-1111-111111111113",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

## 4. Notificações geradas por evento

| Evento | Cliente | Freelancer |
|---|---|---|
| ContratoCriado | não | sim |
| EntregaRegistrada | sim | não |
| ContratoConcluido | sim | sim |
| ContratoCancelado | não | sim |

## 5. Tópicos de erro (DLT)

Mensagens que esgotam as tentativas (4 retentativas com backoff exponencial de 1 s a 8 s) são publicadas, com o mesmo key e value, em:

| Serviço | Tópico |
|---|---|
| auditoria-service | `contrato.eventos.auditoria.DLT` |
| notificacao-service | `contrato.eventos.notificacao.DLT` |
| reputacao-service | `contrato.eventos.reputacao.DLT` |

Mensagens malformadas (JSON inválido ou campos com formato inválido) vão direto para a DLT, sem retentativa.

## 6. Regras do contrato

- Campos novos podem ser **adicionados** como opcionais. Remover ou renomear campos existentes quebra os consumidores.
- O `eventId` é único por evento e **não muda em reenvios**.
- A ordem dos eventos só é garantida **por contrato** (mesma chave, mesma partição). Não há garantia de ordem entre contratos diferentes.
- Os consumidores devem ignorar, sem tratar como erro, os eventos que não lhes interessam.
- Os consumidores devem ser idempotentes pelo `eventId`.
- A mensagem não deve carregar dados sensíveis.