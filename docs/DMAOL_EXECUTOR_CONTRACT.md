# Contrato mínimo DMAOL ↔ Executor v3 — FIRST CONTACT, Fase 3B

Estado: propuesta documental; no implementada ni autorizada para ejecución.
Baseline inspeccionado: `e8c37ed7ea4f4df5f60891ac0f3a3395e3aafeba`, rama
`codex/executor-v3`. No se inspeccionó ni modificó el workspace de DMAOL.
Los nombres de estados DMAOL siguientes son normativos para esta propuesta;
DMAOL deberá confirmar su correspondencia con su modelo real.

## 1. Frontera actual y arquitectura propuesta

El código existente `executor/sessions.py` ofrece exclusivamente:

- `create SESSION_ID`: crea una sesión y emite `SESSION_CREATED:<nombre>`.
- `destroy SESSION_ID`: cierre administrativo y limpieza; emite
  `SESSION_DESTROYED:<nombre>`. Puede dejar cierre parcial o incierto.
- `recover SESSION_ID`: diagnóstico JSON no destructivo. Puede consultar Docker;
  no es una operación sin dependencias externas.

Referencias: `main`, `create`, `cleanup`, `recovery_plan`, `lifecycle_lock`,
`prepare_workspace` y `verify_survival` en el baseline. Las fases internas son
`prepared`, `spawning`, `gateway`, `container`, `starting`, `active`, `stopping`,
`closed`. No existe API de tareas, recepción de payload, resultado de trabajo,
idempotency key, fencing distribuido ni protocolo DMAOL en este módulo.

Se propone un adaptador futuro, único escritor autorizado por deployment, entre
DMAOL y el CLI. Debe tener registro durable propio de operaciones, generaciones,
resultados y tombstones. No se propone modificar el journal interno del Executor.
El transporte queda fuera del alcance; debe autenticar ambos extremos y autorizar
por deployment/tarea/operación. JSON no constituye autenticación. No se permite
que DMAOL invoque Docker directamente ni que construya comandos shell.

Crear una sesión no entrega una tarea al agente. El inicio de trabajo requiere
un canal de despacho y de resultados todavía inexistente en este contrato físico.
Hasta verificarlo, `start` debe rechazar con `DEPENDENCY_UNVERIFIED`, sin crear
recursos. `ACTIVE` sólo describe recursos observados, nunca éxito de la tarea.

## 2. Identificadores e invariantes

| Campo | Tipo propuesto | Responsabilidad |
| --- | --- | --- |
| `task_id` | UUID canónico no nulo | DMAOL; identidad lógica estable |
| `attempt_id` | UUID canónico no nulo | Nuevo en cada intento; nunca reutilizable |
| `session_id` | `s-` + 32 hex minúsculos aleatorios | Adaptador; una sesión exclusiva por intento, nunca reciclada |
| `deployment_id` | 32 hex minúsculos | Deployment aprobado; coincide con configuración v3 |
| `generation` | entero JSON de 1 a 9007199254740991, no bool | Autoridad durable de DMAOL; crece por tarea |
| `request_id` | UUID canónico no nulo | Clave idempotente de una operación |
| `operation_id` | UUID canónico no nulo | Adaptador; operación durable aceptada |
| `executor_instance` | 32 hex o null antes de conocerlo | Correlación con `record.instance`, no generación |
| `state_version` | entero positivo seguro | Revisión monotónica del registro del adaptador |

La tupla `(deployment_id, task_id, attempt_id, generation, session_id)` se vincula
antes de cualquier efecto. El `session_id` satisface la gramática real del CLI:
`[a-z0-9][a-z0-9-]{0,39}`. Nunca se deriva una ruta de texto libre de una tarea.
`record.instance`, boot ID, PID/ticks y container ID identifican recursos locales;
ninguno sustituye una generation ni autentica un resultado de tarea.

## 3. Operaciones mínimas del adaptador

| Operación | Semántica y efecto permitido |
| --- | --- |
| `start` | Validar autorización/generation; registrar intención y asignación durable antes de `create`; después, despachar una sola tarea por el canal verificado. Respuesta inicial `ACCEPTED` no implica creación ni ejecución confirmadas. |
| `observe` | Consultar operación/asignación o pedir diagnóstico no destructivo. No crea, adopta, reinicia ni destruye. Siempre informa fecha, versión y fuente de la observación. |
| `cancel` | Registrar intención de cancelar para la tupla exacta. Sólo solicitar `destroy` con permiso explícito de cierre y evidencia de identidad; reconocer solicitud no confirma salida. |
| `recover` | Reconciliar registro durable y diagnóstico de una sesión ya vinculada. No reasigna, relanza, concede generaciones ni ejecuta `destroy`. Si falta evidencia: `UNKNOWN`/revisión humana. |

`observe` admite consulta sólo por `request_id` original cuando se perdió el ACK de
`start`; evita exigir al cliente el `session_id` que todavía no recibió. Las demás
acciones sobre recursos requieren la tupla completa. No hay búsqueda global ni
barrido de sesiones como parte de este contrato mínimo.

## 4. Estados: separar tarea, recursos y certeza

La respuesta mantiene tres ejes; no comprimirlos en un único estado de éxito:

- `task_state`: `ACCEPTED`, `DISPATCHING`, `RUNNING`, `CANCEL_REQUESTED`,
  `SUCCEEDED`, `FAILED`, `CANCELLED` o `UNKNOWN`.
- `resource_state`: `NOT_CREATED`, `CREATING`, `ACTIVE_OBSERVED`, `STOPPING`,
  `CLOSED_CONFIRMED` o `UNKNOWN`.
- `certainty`: `CONFIRMED`, `OBSERVED` o `UNKNOWN`. `OBSERVED` es una instantánea,
  no garantía de supervivencia futura; una respuesta almacenada no es observación viva.

| Evidencia Executor | Mapeo admisible |
| --- | --- |
| Intención de start durable sin efecto iniciado | tarea ACCEPTED; recursos NOT_CREATED |
| prepared/gateway/container, sin incidencias | recursos CREATING; no inferir tarea RUNNING |
| spawning o `docker_uncertain=true` | recursos UNKNOWN; detener automatismos |
| starting o `start_attempted=true` sin confirmación | posible trabajo iniciado; UNKNOWN, conservar |
| active y supervivencia observada | recursos ACTIVE_OBSERVED; tarea RUNNING sólo con ACK autenticado de despacho |
| stopping | recursos STOPPING; no confirma cancelación |
| closed, o cierre conciliado con evidencia durable propia | recursos CLOSED_CONFIRMED; no determina resultado de tarea |
| recover absent | ausencia observada local; nunca prueba que una tarea previa no produjo efectos |
| recover quarantine/operator-review | revisión requerida; no autoriza acciones mutantes |

Transición normal de tarea: ACCEPTED → DISPATCHING → RUNNING → SUCCEEDED/FAILED.
Desde un estado no terminal se puede solicitar CANCEL_REQUESTED; CANCELLED exige
salida confirmada y conciliación del canal de resultados. Si un resultado válido se
confirmó antes de aceptar la cancelación, se conserva SUCCEEDED/FAILED y se informa
`ALREADY_TERMINAL`; cerrar recursos es una decisión separada. Si el orden no puede
probarse, no declarar CANCELLED: usar UNKNOWN/revisión. El adaptador serializa estas
decisiones mediante transacción y `state_version`.

Una discrepancia puede llevar cualquier operación no resuelta a UNKNOWN. La
recuperación necesita evidencia para salir de UNKNOWN; no usa el simple paso del
tiempo. Una terminación de recursos no convierte una tarea en FAILED ni SUCCEEDED.
Los resultados terminales aceptados son inmutables; las observaciones posteriores
pueden marcar incertidumbre en recursos sin reescribir el resultado de la tarea.

## 5. Mensajes propuestos

Todos son objetos JSON UTF-8, versión exacta `dmaol-executor/1`, máximo 16384 bytes
por mensaje de control; rechazar claves duplicadas/desconocidas, tipos coercibles,
NaN/Infinity y campos de identidad inválidos. Los artefactos viajan por referencia
aprobada y hash, no como logs, credenciales o payload arbitrario en este canal.
Cada operación tendrá esquema exacto; no aceptar campos del ejemplo en operaciones
a las que no pertenecen. No transportar rutas, comandos, variables de entorno,
Docker endpoints, configuraciones o imágenes elegidos por DMAOL.

Solicitud inicial de start (session_id es null porque lo asigna el adaptador):

```json
{
  "version": "dmaol-executor/1",
  "op": "start",
  "request_id": "11111111-1111-4111-8111-111111111111",
  "deployment_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "task_id": "22222222-2222-4222-8222-222222222222",
  "attempt_id": "33333333-3333-4333-8333-333333333333",
  "generation": 7,
  "session_id": null,
  "authorization_ref": "approval-lab-001",
  "profile_ref": "isolated-task-v1",
  "input_ref": "artifact-input-001",
  "input_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "execution_timeout_ms": 60000
}
```

Respuesta durable de aceptación (ejemplo de interfaz futura, no salida actual CLI):

```json
{
  "version": "dmaol-executor/1",
  "request_id": "11111111-1111-4111-8111-111111111111",
  "operation_id": "44444444-4444-4444-8444-444444444444",
  "deployment_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "task_id": "22222222-2222-4222-8222-222222222222",
  "attempt_id": "33333333-3333-4333-8333-333333333333",
  "generation": 7,
  "session_id": "s-cccccccccccccccccccccccccccccccc",
  "executor_instance": null,
  "state_version": 1,
  "task_state": "ACCEPTED",
  "resource_state": "NOT_CREATED",
  "certainty": "CONFIRMED",
  "observed_at": "2026-10-22T12:00:00Z",
  "source": "adapter-ledger",
  "error": null
}
```

Cancelación: mismo envelope de solicitud con `op=cancel`, nuevo `request_id`, tupla
completa, `authorization_ref` y `reason_code=OPERATOR_REQUEST`; sin campos de
input/profile/timeout. `observe`/`recover` usan nuevo request_id, tupla y aprobación
de lectura; observe por ACK perdido sustituye la tupla por `lookup_request_id`.
El error de una respuesta conserva el envelope y estados conocidos:

```json
{
  "code": "OUTCOME_UNKNOWN",
  "operation": "start",
  "effect": "MAY_HAVE_OCCURRED",
  "retry": "OBSERVE_SAME_OPERATION",
  "operator_review_required": true,
  "executor_error_code": "CREATE_INCOMPLETE_REVIEW_REQUIRED"
}
```

Códigos mínimos: INVALID_REQUEST, UNAUTHORIZED, APPROVAL_EXPIRED,
DEPENDENCY_UNVERIFIED, STALE_GENERATION, IDEMPOTENCY_CONFLICT, BUSY,
ALREADY_TERMINAL, OUTCOME_UNKNOWN y RESOURCE_IDENTITY_UNVERIFIABLE.
INVALID_REQUEST/UNAUTHORIZED previos al efecto indican `effect=NONE`; un error de
transporte o del CLI no permite inferirlo. Mensajes no incluyen stderr crudo ni
secretos. El traductor de errores del CLI debe mantener lista explícita; excepciones
no reconocidas se clasifican UNKNOWN. `LIFECYCLE_LOCK_TIMEOUT` sólo permite BUSY/sin
efecto cuando consta que esa invocación no inició efectos; no invalida otra operación.

## 6. Fencing y resultados antiguos

DMAOL debe mantener la generation vigente en una autoridad durable con actualizaciones
linealizables por tarea; el adaptador verifica esa autoridad antes de aceptar una
mutación. No basta con recibir un entero mayor del solicitante ni con almacenar
sólo el mayor valor visto por un adaptador atrasado. Revocar un intento y avanzar
generation debe ser una decisión atómica de esa autoridad.

Todo resultado incluye la tupla completa, result_id UUID, outcome, artifact_ref,
artifact_sha256 y origen autenticado ligado al intento. Su aceptación/publicación
se hace en una transacción que compara generation e intento actuales y comprueba
que no haya resultado terminal previo. La reasignación y la publicación comparten
esa serialización: un resultado de generation 7 no puede publicarse tras avanzar a 8.
Duplicados idénticos devuelven el resultado previo; mismo result_id con contenido
distinto se rechaza. Resultados atrasados se conservan como evidencia segregada,
sin aplicarlos al workspace nuevo ni cambiar el estado de la nueva asignación.

Executor v3 no implementa ese fencing. Incluso un adaptador correcto no impide
que una carga vieja continúe escribiendo en su workspace. Por eso cada intento
usa workspace distinto y ningún agente publica directamente en destinos compartidos.
Si hay efectos externos, su receptor debe verificar generation atómicamente también;
sin esa capacidad, la reasignación queda bloqueada hasta confirmar cese/aislamiento
del intento anterior. No se promete ejecución ni efectos «exactly once».

## 7. Timeout, cancelación y reinicio

Distinguir espera de respuesta, plazo de ejecución y cierre. El cliente que agota
su espera consulta por request_id: no genera otro start. Los límites actuales del
Executor son lock 5 s, gateway 5 s y cada comando Docker 15 s; no son un deadline
total ni un timeout de tarea. I/O de kernel/almacenamiento puede bloquear.

execution_timeout_ms se valida contra un perfil aprobado; no habilita kill ni
limpieza. El supervisor futuro inicia el plazo al confirmar despacho, usa reloj
monotónico local y persiste además la política/deadline para reinicio. Tras un
reinicio, si no puede reconstruir con confianza el plazo o la identidad, marca
UNKNOWN y concilia; nunca concede tiempo adicional/reinicia por defecto. Al vencer
el plazo registra intención de cancelar sólo si el permiso lo contempla; cualquier
cierre requiere verificar identidad y conservar datos. No hay escalado automático
a SIGKILL, rm -f ni borrado de workspace.

El adaptador registra request_id, hash canónico de solicitud, tupla y operación
antes de invocar el CLI. Reintento idéntico devuelve la misma operación; la misma
clave con otros campos devuelve IDEMPOTENCY_CONFLICT. También se impone unicidad de
(task_id, attempt_id, generation): otra clave no puede crear una sesión duplicada.
Una tabla durable de generación vigente/tombstones impide reutilizaciones aun si
el Executor elimina su journal después del cierre. No se prometen garantías tras
purgar esa tabla; su retención es condición de integración.

Tras caída entre intención y respuesta, conciliar con recover; no repetir create a
ciegas. `CREATE_INCOMPLETE_REVIEW_REQUIRED`, `CREATE_ROLLBACK_INCOMPLETE`, spawning,
docker_uncertain y errores de identidad bloquean acciones automáticas. `SESSION_EXISTS`
no es éxito de idempotencia: verificar vínculo completo antes de reconocerlo.
Ausencia de journal tampoco prueba que no se ejecutó la tarea. Recover nunca
reenvía el trabajo ni reconoce orfandad a partir de `bridge_liveness=unknown`.

## 8. Workspace y autorización humana

El workspace es `<workspace_root aprobado>/<session_id>` y lo asigna el adaptador,
no el cliente. Un intento nuevo usa un session_id nuevo; el código actual puede
reutilizar un directorio de workspace existente con dueño/modos válidos, por lo
que el adaptador debe rechazar esa reutilización. Estado, runtime, workspace legacy
y v3 permanecen separados, sin aliases ni rutas solapadas. Verificar ancestros,
propietarios, permisos y referencias por descriptor; prohibir symlinks/bind mounts
no aprobados. El agente no modifica directorios de control del adaptador.

Entrada/materialización y exportación de resultados requieren un canal específico,
verificación de hashes y política de archivos/enlaces; nunca copiar árboles vivos
entre intentos. `destroy` conserva el workspace, pero puede detener escritores y
eliminar el contenedor: resultados necesarios deben salir de su capa efímera mediante
un procedimiento verificado. No se considera seguro aceptar un resultado cuya ruta
mutable pueda sustituirse después de verificar su hash; publicar artefacto inmutable.

Una aprobación humana debe acotar entorno independiente, deployment, tarea/intento,
generation, perfil, recursos, duración y acciones permitidas. `authorization_ref`
identifica ese registro aprobado; no es un secreto ni una autorización por sí sola.
Antes de cada efecto se verifica vigencia y alcance en el lado confiable. Cancelar
no concede permiso para destruir otra sesión; una cancelación atrasada se rechaza
como STALE_GENERATION. Cerrar un intento antiguo requiere una decisión administrativa
separada ligada a su identidad original, sin mutar el nuevo intento.

Esta documentación no concede permiso para ejecutar nada. Laboratorio, integración,
cierre destructivo, migración, cambios de servicios y despliegue requieren sus
respectivas aprobaciones. No acceder a producción para verificar este contrato.

## 9. Bloqueos reales y criterios verificables

1. **Adaptador/autoridad no implementados:** ledger durable, unicidad, fencing
   linealizable, autenticación/autorización y traductor de errores son requisitos
   futuros. Probar ACK perdido, solicitud duplicada y reinicio sin segundo create.
2. **Canal de tareas/resultados no verificado:** el CLI crea recursos, no ejecuta
   una tarea DMAOL por contrato. Verificar despacho, identidad del emisor, finalización,
   exportación de datos y rechazo transaccional de resultados/cancelaciones atrasados.
3. **Supervisión y recursos reales pendientes:** pidfd, Docker ambiguo, supervivencia,
   permisos entre usuarios, cierre/flush y persistencia ante crash requieren laboratorio
   independiente. IRONCLAD Fase 2C sólo habilita L00; L01 y fixtures activos siguen
   bloqueados. Ningún mock sustituye esa evidencia ni demuestra deadline total.
4. **Encaje operativo no verificado:** confirmar modelo real DMAOL, canal Bridge/Node,
   readiness funcional del gateway, aislamiento del daemon/perfiles, bootstrap de
   raíces y fencing del ejecutor antiguo. Las rutas/config actuales son de despliegue;
   no usarlas para experimentar ni copiar credenciales. Recover depende de raíces y
   configuración válidas, incluidas las legacy, y del daemon accesible.

Antes de una integración real deben comprobarse estos criterios: una sola asignación
vigente por tarea; resultado viejo rechazado aun con carrera contra reasignación;
ningún nuevo efecto por reintento tras ACK perdido; cancelación incierta nunca
publicada como cierre confirmado; reinicio sin relanzamiento automático; workspace
anterior conservado y sin acceso al nuevo; toda acción limitada a recursos aprobados.
Son criterios propuestos, no pruebas ejecutadas ni capacidades declaradas existentes.

Referencias locales: `executor/sessions.py`, `docs/RECOVERY_DESIGN.md`,
`docs/DEPLOYMENT_PLAN.md` y sección vigente Fase 2C de `lab/ironclad/README.md`.
