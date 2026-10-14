package br.com.freela.contrato.infrastructure.controller;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.UUID;

public record CriarContratoRequest(
        @NotNull UUID clienteId,
        @NotNull UUID freelancerId,
        @NotBlank @Size(max = 160) String titulo,
        @NotNull @DecimalMin(value = "0.01") BigDecimal valor) {
}
