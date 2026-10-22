# CROSSWIRE LIVE — Cliente Experimental DMAOL para FORGE LINK
## Fase 3D-E / 3D-E2

**Responsable:** Antigravity (AG) — Arquitecto de Sistemas e Integración
**Rama:** `ag/architecture`
**Referencia FORGE LINK:** `654737f9066f8f03141cb76e396f2fde0b1131f0` (en `origin/codex/executor-v3`)

---

### 1. Propósito y Alcance

Este componente experimental implementa el cliente DMAOL (`DmaolLiveClient`) diseñado para comunicarse con el servidor auténtico **FORGE LINK** implementado por Codex, bajo la especificación contractual `dmaol-executor/1` (`docs/DMAOL_EXECUTOR_CONTRACT.md`).

El cliente:
- Reutiliza el adaptador estricto existente `DmaolExecutorAdapter`.
- Gestiona las operaciones contractuales `start`, `observe`, `cancel` y `recover`.
- **No crea una autoridad de ejecución adicional**: la autoridad contractual reside exclusivamente en `ExecutionLedger` de FORGE LINK; DMAOL administra únicamente el workflow y la expiración de leases de tareas.
- **Preserva estados inciertos**: si una respuesta o recuperación retorna `OUTCOME_UNKNOWN` o `UNKNOWN`, se mantiene como incierto (`QUARANTINED` en DMAOL) y jamás se convierte artificialmente en éxito.
- **Rechaza resultados obsoletos**: cualquier intento de reconciliación con generación menor a la vigente eleva `StaleGenerationError`.
- **Valida integridad de artefactos**: verifica en memoria que los bytes leídos produzcan exactamente el hash SHA-256 contractual reportado (`acad81346d1f2213d8236cedd2a4f8220361f6f189d120d667ba8e9eab5da8ea`).
- **No inventa datos sintéticos**: si el ejecutor no reporta un commit Git, se preserva `commit_sha = None`. Si la duración no fue medida por el canal de control, se registra el marcador canónico `0.0` sin falsificar mediciones.

---

### 2. Distinción Rigurosa: Ledger Auténtico vs. Ejecución Física

| Dimensión | En entorno de desarrollo Windows 11 (AG) | En entorno Ubuntu 24.04 (Codex) |
| :--- | :--- | :--- |
| **Autoridad Contractual (`ExecutionLedger`)** | **Ejecutado y verificado (100% éxito)**: el ledger SQLite es Python estándar y opera idénticamente en cualquier plataforma. | Totalmente soportado y probado por Codex. |
| **Esquemas JSON `dmaol-executor/1`** | **Ejecutado y verificado**: validación estricta de 5-tuplas, idempotencia, fencing y formato. | Totalmente soportado. |
| **Ejecución Física de Proceso (`ProcessRunner`)** | **No ejecutable directamente**: depende de interfaces exclusivas del kernel Linux (`os.geteuid()`, `/proc/<pid>/stat`, `boot_id`, `os.O_NOFOLLOW`, `os.set_blocking` en pipes). | **Totalmente soportado**: ejecuta el programa fijo real `echo_program.py` (`isolated-echo-v1`). |

---

### 3. Instrucciones de Ejecución en Ubuntu 24.04 junto al commit `654737f` de Codex

Para ejecutar este cliente junto a la implementación auténtica de FORGE LINK en un host o VM Ubuntu 24.04 (usuario no root, Python 3.12+):

```bash
# 1. Asegurar que las ramas remotas estén al día
git fetch origin

# 2. En el espacio de trabajo con ag/architecture, o exportando ambos componentes:
# (Alternativa sin merge: clonar o extraer el lab de codex a un directorio temporal)
python3 -B -m unittest discover -s tests/crosswire_live -t . -v

# 3. Para ejecutar la suite completa de DMAOL:
python3 -B -m unittest discover -s tests/orchestration -t . -v
```

### 4. Estructura de Archivos
- `lab/crosswire_live/client.py`: Implementación de `DmaolLiveClient`.
- `lab/crosswire_live/transport.py`: Implementación de `ForgeLinkDirectTransport`.
- `tests/crosswire_live/test_crosswire_live.py`: Suite de 8 pruebas de aceptación contractual contra `ExecutionLedger` del commit `654737f`.
