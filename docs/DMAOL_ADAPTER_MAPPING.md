# DUALITY CORE — Mapeo de Identidades y Adaptador de Integración (Fase 3B-A)
**Contrato `dmaol-executor/1` ↔ DMAOL**

**Fecha:** 2026-10-21  
**Rama:** `ag/architecture`  
**Baseline:** `945a745f00844610fba87ab5fec9e245b9d31f9c`  
**Estado:** Experimental / En desarrollo. **No apto para producción.**

---

## 1. Mapeo y Correspondencia de Identidades

| Campo | DMAOL (`TaskEnvelope` / Almacén) | Contrato (`dmaol-executor/1`) | Autoridad de Asignación / Incremento |
|---|---|---|---|
| **`task_id`** | Identificador de agregado (ej. `task-uuid` o string determinista). | UUID canónico en minúsculas (`[0-9a-f]{8}-...`). Mapeado deterministamente mediante `uuid5` si el ID de DMAOL no es UUID canónico. | **DMAOL** (al crear la tarea). Inmutable durante todo el ciclo de vida. |
| **`attempt_id`** | No existía en el aggregate base. Se introduce en el adaptador. | UUID canónico en minúsculas. Distinto y único por cada ciclo de lease / reintento. | **DMAOL / Adaptador** (al iniciar una tentativa de despacho). Nunca se reutiliza ni se recicla. |
| **`fencing_token` / `generation`** | Entero positivo incremental (`task.fencing_token`) gestionado por `LeaseManager`. | Entero JSON de 1 a $9007199254740991$ (`generation`). Ambos conceptos representan la misma época monótona de arrendamiento. | **Exclusivamente DMAOL (`LeaseManager`)**, bajo exclusión mutua de kernel (`CrossPlatformFileLock`). Ni el Executor ni el Adaptador pueden inventar ni incrementar la generación. |
| **`session_id`** | N/A en modelos DMAOL de alto nivel. | `s-` + 32 caracteres hexadecimales en minúsculas (`^s-[0-9a-f]{32}$`). | **Exclusivamente el Adaptador**. El cliente DMAOL envía `"session_id": null` en la solicitud `start`, y el ledger del adaptador provisiona un ID de sesión fresco y exclusivo para ese intento. |
| **`request_id`** | N/A (generado por cliente). | UUID canónico en minúsculas. Clave de idempotencia para cada operación de control (`start`, `observe`, `cancel`, `recover`). | **Cliente / Invocador**. Si se reenvía la misma clave con el mismo payload, se reemite la respuesta previa; con payload divergente, arroja `IDEMPOTENCY_CONFLICT`. |
| **`state_version`** | `TaskEnvelope.state_version` (versión del aggregate). | `state_version` del registro del Adaptador sobre la tupla `(deployment_id, task_id, attempt_id, generation, session_id)`. | **Ledger del Adaptador**. Se incrementa en cada mutación o actualización de recursos observados para control de concurrencia optimista (OCC). |

---

## 2. Relación entre `generation` y `fencing_token`

1. **No equivalencia por asunción:** `generation` en el contrato no se asume equivalente a un simple contador de ejecuciones. Se demuestra que corresponde al fencing token emitido por la autoridad durable de leases de DMAOL:
   $$\text{generation} = \text{task.fencing\_token}$$
2. **Autoridad y Dirección:**
   - La autoridad de incremento reside exclusivamente en DMAOL (`LeaseManager`).
   - El Adaptador valida que la `generation` en toda operación (`start`, `observe`, `cancel`, `recover`, y entrega de resultados) sea igual a la generación activa en el ledger.
   - Cualquier solicitud o resultado con una generación inferior a la activa es rechazado con `STALE_GENERATION`.

---

## 3. Separación de Tarea, Recursos y Certeza

El adaptador mantiene explícitamente tres ejes ortogonales en sus respuestas, evitando el falso colapso a un único booleano de éxito:

1. **`task_state`:**
   `ACCEPTED` $\rightarrow$ `DISPATCHING` $\rightarrow$ `RUNNING` $\rightarrow$ `SUCCEEDED` / `FAILED` / `CANCELLED` (o `UNKNOWN`).
2. **`resource_state`:**
   `NOT_CREATED` $\rightarrow$ `CREATING` $\rightarrow$ `ACTIVE_OBSERVED` $\rightarrow$ `STOPPING` $\rightarrow$ `CLOSED_CONFIRMED` (o `UNKNOWN`).
3. **`certainty`:**
   `CONFIRMED`, `OBSERVED`, `UNKNOWN`.

---

## 4. Manejo de Fallas y Casos Negativos Comprobados

- **ACK Perdido:** Se recupera consultando `observe` con `lookup_request_id` sin duplicar el aprovisionamiento de recursos.
- **Reinicio entre Intención y Respuesta:** El ledger persiste la intención en disco con fsync antes de interactuar con el Executor. Tras caída, se consulta el estado sin reiniciar recursos ciegamente.
- **Resultado de Intento Anterior / Generación Obsoleta:** Rechazado con `STALE_GENERATION`. El payload se conserva como evidencia segregada en `results/segregated_*.json` para auditoría, sin tocar la tupla activa.
- **Cancelación Tardía:** Si la tarea ya alcanzó un estado terminal (`SUCCEEDED`, `FAILED`, `CANCELLED`), la cancelación es rechazada con `ALREADY_TERMINAL`.
- **Límites Estrictos de Mensaje:** Se rechazan cargas superiores a 16.384 bytes, claves duplicadas en JSON y tipos de datos coercibles.
