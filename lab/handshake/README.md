# Executor Contract Simulator — Fases 3B-B / 3C-B

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


## CROSSWIRE — hardening 3C-B

Baseline de esta fase: `c44af038c54135a03fb1e695a375b1d13f57c456`.
Se conserva `dmaol-executor/1`, las cuatro operaciones y los fixtures de intercambio
3B-B sin cambios. No se añade protocolo de transporte ni servidor de red. Los nuevos
controles verifican almacenamiento interno; no crean nuevos mensajes wire.

### Defectos corregidos

La apertura previa ejecutaba INSERT OR IGNORE incluso sobre un archivo existente:
una fila model eliminada podía convertirse silenciosamente en un ledger nuevo,
perdiendo idempotencia y tombstones. Ahora sólo se inicializa una ruta inexistente.
Un archivo existente vacío, sin tabla/fila, truncado o de otro esquema se rechaza;
no se recrea ni se usa como ledger nuevo. Ante fallo de apertura se cierra la conexión.

Antes se aceptaba casi cualquier JSON del ledger: sólo se comprobaba deployment al
abrir y state_version al escribir. Ahora se ejecuta quick_check al abrir, y se
valida el estado completo al leer y antes de confirmar una transacción: esquema,
tipos, estados, identidades, versiones, autoridad, tombstones, vínculo de solicitudes
con asignaciones y resultados terminales/segregados. JSON duplicado, no finito o
inconsistente provoca `LedgerCorrupt` (subclase de ValueError). Fallos físicos de
SQLite conservan sus excepciones originales. No se devuelve ACCEPTED ni se inventa
UNKNOWN como sustituto de evidencia perdida. No existe reparación automática.

### Evidencia ejecutable

La suite completa se ejecuta con el comando de arriba: **36 pruebas**, de las cuales
15 son nuevas en `tests/handshake/test_hardening.py`. El worker auxiliar
`tests/handshake/process_worker.py` sólo abre la SQLite temporal indicada por la
prueba. Cada worker es un intérprete Python independiente, con conexión propia;
no hereda conexiones ni usa Docker, Executor, sockets, red o servicios. La barrera,
las respuestas y los logs son archivos temporales regulares. Los timeouts del harness
sólo limitan la prueba y pueden terminar exclusivamente su propio worker descartable.

| Prueba | Garantía observada en este laboratorio |
| --- | --- |
| Cuatro procesos con mismo start, antes/después de reabrir | Misma respuesta durable y una sola sesión |
| Cuatro request_id distintos para el mismo intento | Una aceptación; el resto IDEMPOTENCY_CONFLICT |
| Mismo request_id, contenido distinto, simultáneo | Una aceptación y un conflicto; sin sobrescritura |
| Salida abrupta antes de COMMIT con UPDATE SQL pendiente | SQLite revierte la transacción; ningún registro parcial visible |
| Salida abrupta después de COMMIT y antes del ACK | Reintentos desde otros procesos recuperan la respuesta original |
| Duplicados y resultados terminales contrapuestos simultáneos | Un solo terminal aceptado; duplicado histórico o rechazo sin reescritura |
| Cancel/result/reassign simultáneos y sus seis órdenes seriales | Resultado se acepta antes de revocación/cancelación, o se rechaza/queda incierto; nunca cambia el intento nuevo |
| Resultado/cancelación antiguos tras terminal nuevo y reapertura | Conserva el terminal vigente; evidencia tardía segregada |
| Dos deployments y dos intentos con IDs/referencias deliberadamente coincidentes | No cruzan asignaciones, aprobaciones, lookup, cancelaciones ni resultados |
| JSON/filas/esquemas/identidades/versiones/resultados corruptos | Rechazo de apertura y de operaciones en conexiones ya abiertas, sin reconstrucción |
| Bloqueo SQLite hasta agotar espera | Excepción local sin mutación; reintento posterior mantiene idempotencia |

La recuperación conservadora demostrada consta de dos casos distintos: rollback
nativo de una transacción interrumpida, y restauración **explícita por la prueba** de
los datos intactos que se conservaron antes de una corrupción inyectada. Después se
reabre y verifica la respuesta histórica, sin segundo start. Esto no es un mecanismo
de backup/restore de producción. No se acredita restaurar snapshots atrasados, perder
el ledger, ni descubrir operaciones cuyo registro durable desapareció.

### Límites de estas garantías

Los procesos concurren sobre un ledger **preinicializado**. El bootstrap simultáneo
de una ruta nueva no está soportado: se debe crear una vez antes de lanzar workers;
una carrera de apertura puede rechazarse de forma conservadora. Se presupone que la
ruta del ledger permanece estable. Sustitución concurrente del archivo, symlinks,
permisos del host y almacenamiento remoto no están cubiertos.

Las validaciones detectan las corrupciones estructurales y relacionales ensayadas;
no autentican el archivo ni detectan toda modificación coherente, rollback de un
snapshot completo, pérdida de tombstones no vinculados o manipulación maliciosa.
quick_check tampoco demuestra salud del hardware. No hay migración ni salvamento
parcial. La validación recorre el ledger completo; no se ha probado escalabilidad.

Las carreras ejercitan órdenes reales admitidos por SQLite local, y los seis órdenes
seriales dan cobertura determinista del modelo. No constituyen una prueba formal de
todos los interleavings, autoridad distribuida, fencing externo, corte de energía o
fallos de kernel. La autoridad de reasignación sigue siendo un control ficticio:
no confirma aislamiento/cese real antes de avanzar generación. Un ACK ACCEPTED de
resultado repetido tras revocación describe una decisión histórica, **no publicación
nueva**. La prueba verifica que el intento actual permanezca intacto.

No se traduce sqlite3.OperationalError de lock timeout a BUSY del protocolo: sigue
siendo fallo local del simulador, sin ACK de efecto. No se ha validado el cliente
DMAOL real ni una especificación CROSSWIRE adicional: compatibilidad aquí significa
conservar el contrato local y pasar los fixtures y las 21 pruebas 3B-B previas.
Autenticación, políticas reales, infraestructura y exactly-once siguen fuera de
alcance. Esta fase no habilita ninguna integración, despliegue ni auditoría adicional.


## FORGE LINK 3D-C2 — autoridad compartida

La implementación durable se extrajo a `ledger.py`. `simulator.py` conserva la
fachada Simulator, sus importaciones públicas, constructor y respuestas previas;
las 36 pruebas originales siguen ejecutándose sin cambios. El simulador sigue
sin lanzar procesos y conserva su política aislada `isolated-task-v1`.

`ExecutionLedger` reutiliza esa misma autoridad y transacción para IGNITION y FORGE
LINK. Añade tablas de hechos de proceso y snapshots de artefactos en el **mismo**
archivo SQLite, sin otra tabla de generaciones. No admite simulate_evidence ni
simulate_result. El perfil real usa `isolated-echo-v1` y sólo input vacío con una
referencia fija aprobada. La integración y sus límites se describen en
[FORGE LINK](../forge_link/README.md). No se alteró el protocolo ni sus fixtures.
