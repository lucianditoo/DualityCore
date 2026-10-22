# Executor Contract Simulator — Fase 3B-B

Modelo experimental **offline**, para Python 3.12 y biblioteca estándar. Referencia:
[DMAOL_EXECUTOR_CONTRACT.md](../../docs/DMAOL_EXECUTOR_CONTRACT.md).
Baseline inspeccionado: `01279ff4833907ab503333665a24a84ebbf4b4c7`, rama
`codex/executor-v3`. No modifica ni importa `executor/sessions.py`.

El Executor actual implementa `create`, `destroy` y `recover`, con creación de
recursos, cierre conservador y diagnóstico. No implementa tareas DMAOL, despacho,
resultados, generaciones ni request IDs. El simulador representa exclusivamente
el ledger de un adaptador futuro. Sus únicos efectos son escribir el archivo
SQLite **de prueba** que proporciona el llamador y devolver datos. No tiene backend
Executor, comandos, sockets, servicios, Docker ni acceso a recursos de sesiones.
Usar siempre un directorio temporal independiente; nunca una ruta de producción.

## Uso reproducible

Desde la raíz del repositorio:

```sh
python3 -B -m unittest discover -s tests/handshake -v
```

Ejemplo sin infraestructura (todos los IDs son ficticios):

```python
import json
import tempfile
from pathlib import Path
from lab.handshake.simulator import Simulator

q = json.loads(Path('lab/handshake/fixtures/start.json').read_text())
with tempfile.TemporaryDirectory() as scratch:
    sim = Simulator(Path(scratch) / 'ledger.sqlite', q['deployment_id'],
                    simulated_channel=True)
    try:
        sim.advance(q['task_id'], q['attempt_id'], q['generation'])
        sim.approve(q['authorization_ref'], q['task_id'], q['attempt_id'],
                    q['generation'])
        response = sim.handle(json.dumps(q))
    finally:
        sim.close()
```

`simulated_channel` es falso por defecto: `start` devuelve
`DEPENDENCY_UNVERIFIED`, sin asignación. Activarlo sólo permite asignaciones
**ficticias**; no verifica ningún canal real. Una respuesta `ACCEPTED` sólo registra
una intención; no representa despacho, ejecución ni creación de recursos.

## Interfaz de control

`handle(raw: str | bytes) -> dict` recibe exactamente un objeto JSON UTF-8,
versión `dmaol-executor/1`, hasta 16384 bytes. Rechaza campos extra/faltantes,
claves repetidas, NaN/Infinity, bool/float/string como enteros, UUID no canónicos o
nulos y referencias que puedan representar rutas/comandos. No recibe objetos Python
para impedir que la decodificación oculte claves duplicadas. Referencias son tokens
ASCII de hasta 128 caracteres: letras, números, punto, guion y guion bajo.

Campos comunes: `version`, `op`, `request_id`, `authorization_ref`.
Tupla completa: `deployment_id`, `task_id`, `attempt_id`, `generation`, `session_id`.
UUID canónico no nulo para request/task/attempt; deployment de 32 hex minúsculos;
generation entero de 1 a 9007199254740991.

| Operación | Campos adicionales exactos | Resultado del modelo |
| --- | --- | --- |
| `start` | Tupla, `session_id=null`, `profile_ref`, `input_ref`, `input_sha256`, `execution_timeout_ms` | Vincula una sesión aleatoria `s-` + 32 hex y operación UUID; ACCEPTED / NOT_CREATED / CONFIRMED |
| `observe` | Tupla **o** `lookup_request_id` del start original | Lee ledger; no modifica asignaciones ni crea sesiones |
| `cancel` | Tupla y `reason_code=OPERATOR_REQUEST` | Registra CANCEL_REQUESTED; nunca afirma cierre ni ejecuta destroy |
| `recover` | Tupla | Sin evidencia externa, UNKNOWN y revisión; no relanza, adopta ni cancela |

El perfil ficticio permitido es `isolated-task-v1`, con timeout entre 1 y 60000 ms.
No hay temporizador: este valor es validación de política de ejemplo. Input/hash son
metadatos ficticios; no se leen ni verifican artefactos. La aprobación de lectura
también se exige en lookup y queda ligada al intento original.

La respuesta conserva los campos del ejemplo normativo: versión/request/operation,
tupla, executor_instance, state_version, task_state, resource_state, certainty,
observed_at, source, error. `executor_instance` siempre es null. `operation_id`
identifica la intención start vinculada; las consultas/cancelación/recovery se
registran por su propio request_id. No se pretende definir aquí una identidad
separada de operación de cierre físico.

Antes de existir asignación, operation/session/state_version y los IDs desconocidos
pueden ser null; los tres ejes son UNKNOWN. Es una decisión de esquema local:
el contrato aún no fija el envelope de estos rechazos. Errores no revelan la
asignación cuando autorización o identidad no coinciden.

`source=simulator-ledger` indica exclusivamente evidencia guardada de prueba.
`observed_at` es la fecha del estado del ledger, no una observación viva. Una
respuesta idempotente conserva fecha, versión y estados originales aunque el estado
actual haya avanzado; consultar con un request_id nuevo para leer la versión actual.
OBSERVED sólo puede provenir de evidencia ficticia, jamás de una inspección real.
No se expone estado de tarea como parámetro de las cuatro operaciones.

## Durabilidad y fencing simulados

SQLite con `synchronous=FULL` y `BEGIN IMMEDIATE` serializa autoridad, asignaciones,
solicitudes, cancelaciones y resultados en una transacción. Se confirma antes de
responder. El ledger guarda hash SHA-256 de JSON canónico y respuesta por request_id.
Reintento idéntico devuelve la respuesta original; cambio de contenido produce
IDEMPOTENCY_CONFLICT. Otra clave para el mismo intento tampoco crea una sesión.

`advance` es autoridad local **de fixtures**, independiente de la generation enviada:
revoca el intento anterior y avanza la generación atómicamente. Mantiene tombstones
de todos los attempt_id, incluso si nunca tuvieron sesión; no son reutilizables.
Start/cancel comprueban el intento vigente. Observe/recover pueden consultar un
intento anterior autorizado sin mutar el vigente. Una repetición ya confirmada
reproduce su ACK histórico sin repetir mutaciones, aun tras avanzar generación.

Cada asignación tiene state_version positivo; cada cambio lo incrementa. La versión
no es un campo de solicitud del contrato: enviarla se rechaza. El control de evidencia
sí exige igualdad exacta con la revisión vigente. No hay purga de tombstones ni
autoreconstrucción: un ledger corrupto provoca excepción y detiene el modelo;
una ruta nueva es un laboratorio nuevo, **no recuperación** de un ledger perdido.
Sin el archivo original no existe garantía de no duplicación.

## Controles exclusivos de fixtures (fuera del protocolo)

- `approve(ref, task_id, attempt_id, generation, actions=..., expired=False)` instala
  una aprobación ficticia limitada al deployment configurado, intento y acciones.
  Expiración es un booleano de escenario; no implementa reloj ni registro humano real.
- `advance(task_id, attempt_id, generation)` avanza la autoridad local serializada.
  No verifica cese del intento anterior: no hay cargas ni efectos externos aquí.
- `simulate_evidence(session_id, task_state=..., resource_state=..., certainty=...,
  state_version=...)` admite estados enumerados y transiciones conservadoras.
  DISPATCHING → RUNNING representa un ACK ficticio; no prueba autenticación.
  CANCELLED exige CLOSED_CONFIRMED y CONFIRMED, representando también conciliación
  del canal. UNKNOWN no se resuelve por tiempo ni por reenvío de trabajo.
- `simulate_result(raw)` acepta la tupla y exactamente `result_id` UUID, `outcome`
  SUCCEEDED/FAILED, `artifact_ref`, `artifact_sha256`. Representa un origen confiable
  **simulado**. Valida identidad y generation dentro de la transacción; resultados
  anteriores quedan segregados en `late_results`, sin tocar el intento nuevo.
  Un result_id idéntico reproduce la decisión; contenido distinto se rechaza.
  Resultado en RUNNING se acepta; después de cancelación con orden incierto se usa
  UNKNOWN. Terminal aceptado es inmutable, incluso si recovery duda de recursos.
- `snapshot()` permite inspección de datos ficticios, nunca recursos del host.

## Fixtures y pruebas

`fixtures/start.json` es un mensaje completo reutilizable.
`fixtures/lost_ack_cancel_recover.json` contiene setup, solicitudes completas y
expectativas parciales, con marcador `$session_id` sustituido por la primera respuesta.
`restart` cierra y reabre la misma SQLite; `same_as_step` exige respuesta idéntica.
Fechas y UUID generados no tienen valores fijos. La prueba de intercambio consume
este JSON directamente, sin un servicio ni protocolo de transporte adicional.

Las pruebas usan directorios temporales y cubren validación, scope, expiración,
ACK perdido, reinicio, respuestas repetidas, concurrencia entre conexiones SQLite,
generaciones atrasadas/futuras no autorizadas, sesiones incorrectas, cancelación,
incertidumbre, resultados tardíos/inmutables, rollback y ledger corrupto.
También bloquean Popen, socket y kill al ejecutar las cuatro operaciones. El modelo
no importa módulos del Executor ni dispone de adaptadores conectables.

## Límites y decisiones pendientes de integración

No se detectó incompatibilidad entre las operaciones abstractas implementadas y
los invariantes del contrato. El documento sigue siendo una propuesta: falta fijar
schemas de error previo a asignación, límites de referencias/perfiles y semántica de
operation_id para operaciones posteriores. Esas decisiones locales no constituyen
un esquema aprobado por DMAOL. Las constantes del ejemplo son política de prueba.

**No implementado ni acreditado:** autenticación/autorización real, autoridad DMAOL
distribuida, despacho/resultados reales, Docker/pidfd, identidad/supervivencia del
gateway, terminación/flush, recuperación de orfandad real, aislamiento de workspaces,
exportación inmutable, deadlines, transporte, publicación externa ni exactly-once.
No hay llamadas que puedan crear esos recursos. Las escrituras del ledger son los
únicos efectos; su ruta es responsabilidad del laboratorio. El modelo monolítico
no es un servicio de producción ni una barrera frente a manipulación del archivo.
SQLite presupone almacenamiento local fiable; las pruebas no acreditan corte de
energía, corrupción de disco, backups ni durabilidad de hardware.

El CLI real no satisface la API de tareas; ninguna prueba aquí habilita la
integración. Conectar un Executor, confirmar esquemas con DMAOL o desplegar requiere
trabajo y autorización posteriores. No se crean commits ni se publica esta fase.
