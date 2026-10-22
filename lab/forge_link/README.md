# FORGE LINK 3D-C2 — un contrato, una autoridad, un programa fijo

Baseline: `73f739e356dfbcc39274b6bfd898252bdbbc902f`, rama `codex/executor-v3`.
Sólo laboratorio local, no root. No hay servidor, sockets, HTTP, SSH, red, Docker,
Bridge, servicios ni agentes Codex/Antigravity. No se modifica el Executor real,
IRONCLAD o producción. No se habilita recepción de tareas externas.

## Comandos

```sh
python3 -B -m unittest discover -s tests/handshake -v
python3 -B -m unittest discover -s tests/ignition -v
python3 -B -m unittest discover -s tests/forge_link -v
python3 -B -m lab.forge_link.demo
python3 -B -m lab.ignition.demo
```

La demo FORGE LINK crea únicamente un laboratorio temporal y una aprobación local,
emite JSON start y observe, verifica los bytes publicados contra SHA-256 y presenta
los mensajes contractuales y el resultado. No acepta argumentos ni código/payload.
El cierre del contexto elimina sus archivos temporales después de esperar al proceso.

## Etapas y responsabilidades

1. `lab/handshake/ledger.py` contiene la autoridad extraída del simulador: solicitudes,
   hashes canónicos, sesiones, approvals, generación vigente, tombstones, decisiones
   de cancelación y aceptación de resultados. `simulator.py` conserva la fachada y
   validadores públicos, con la misma interfaz y respuestas offline por defecto.
2. `lab/ignition/process_runner.py` sólo lanza el programa fijo y aporta evidencia
   de identidad, salida, captura y tiempo. No importa el ledger, ni conoce generation,
   aprobaciones contractuales, resultados publicados o tareas vigentes. El backend
   local usa la autoridad extraída; se retiró su segunda tabla generations.
3. `adapter.py` conecta mensajes al mismo componente y al mismo supervisor. No
   sincroniza bases ni mantiene una máquina de estados o validador alternativos.

El archivo SQLite único tiene la fila `model` original y las tablas `runs`,
`artifacts`, `execution_policy`. Las tres últimas son hechos de ejecución, snapshots
verificados de bytes y política local; **no constituyen autoridades de fencing**.
`Ledger.transaction` usa BEGIN IMMEDIATE y synchronous=FULL. La revocación y la
aceptación/publicación del resultado comparten esa transacción y archivo. Cada
operación abre su conexión local; las conexiones no se comparten entre hilos.

No se cambió el esquema JSON de `dmaol-executor/1`: versión, campos y tipos de
solicitudes y respuestas se conservan. `source=adapter-ledger` identifica estados
reales guardados; Simulator sigue usando `simulator-ledger`. No se agregan stdout,
stderr, PID, rutas ni execution_id a los mensajes de control. `executor_instance`
permanece null: no existe record.instance de Executor v3 en este laboratorio.

## Entrada aprobada y API local

`ForgeLab` genera una raíz temporal y deployment propios. `Adapter(root, deployment)`
sólo reabre la raíz temporal existente; no arranca tareas al abrir. `approve` y
`advance` son controles Python confiables de laboratorio, **fuera del protocolo**.
No hay autenticación de red. authorization_ref por sí sola no otorga autorización:
se exige aprobación local vigente con tupla y acción coincidentes. El perfil y sus
límites son política fija del laboratorio, no campos ejecutables elegidos por DMAOL.

La entrada start usa exactamente el esquema normativo, con:

- profile_ref: `isolated-echo-v1`.
- input_ref: `ignition-fixed-input-v1`.
- input_sha256: SHA-256 de bytes vacíos. No se reciben ni materializan otras entradas.
- execution_timeout_ms: entero entre 1 y 5000, sin coerción de bool/float/string.

Estos valores seleccionan exclusivamente `echo_program.py`, ya instalado en el
repositorio. No se admiten comandos, código, cwd, argumentos ni entorno en mensajes.
El programa recibe argv vacío, stdin cerrado, entorno fijo y Python -I -B. El cwd
es una sesión aleatoria nueva bajo la raíz temporal aprobada, no una ruta del mensaje.

`Adapter.handle(raw)` valida con el decodificador/esquema existente. Start persiste
la asignación y el ACK antes de cualquier proceso, reclama durablemente el lanzamiento
y supervisa sincrónicamente al hijo. Devuelve el ACK ACCEPTED original, **no éxito de
tarea**. Observe con nuevo request_id devuelve el estado del ledger; también permite
lookup_request_id si se perdió el ACK. Repetir start conserva el ACK y no inicia otro
proceso cuando hay un claim previo, incluso tras un fallo o recuperación incierta.
No se promete latencia acotada de handle/start. No hay workers de fondo persistentes.

Cancel comprueba la tupla completa, generation y permiso y registra una intención
separada. El supervisor sólo reacciona a esa intención, nunca a UNKNOWN/revocación.
Cancelación previa al claim se confirma sin crear proceso. Después de lanzamiento,
CANCEL_REQUESTED no implica salida. Sólo se declara CANCELLED con salida supervisada
por SIGTERM y sin evidencia contradictoria; salida natural después de ignorar TERM
queda UNKNOWN en el modelo conservador. Un terminal aceptado previamente se conserva.

Recover usa evidencia ya durable y, si ésta falta, marca UNKNOWN sin señales,
adopción, relanzamiento ni cambio de permisos. Tampoco reconstruye plazo concediendo
más tiempo. Un supervisor aún vivo puede aportar luego evidencia que resuelva ese
UNKNOWN. Si ya hay salida verificada, puede confirmar cierre sin cambiar el terminal.
Fechas y versiones describen el ledger, nunca una observación viva garantizada.

## Supervisión e identidad

El supervisor registra PID, boot ID y start ticks del hijo directo y los vincula a
execution_id, session_id y la tupla contractual antes de aceptar evidencia. El claim
SPAWNING se confirma **antes** de Popen. La aprobación y generación se comprueban otra
vez dentro de la transacción que lanza y vincula al hijo: la revocación se serializa
con ese lanzamiento. Una caída antes de persistir identidad deja un claim incierto,
no permiso para repetir. No se señaliza nunca un PID persistido/reabierto.

Sólo Popen del propietario original puede enviar SIGTERM; tras su wait se acepta
exit_confirmed. No hay SIGKILL ni en timeout, cancelación, excepción o finally. Una
excepción no genera cancelación implícita. Se drenan ambos streams y sólo se conservan
4096 bytes de cada uno; el resto se cuenta y descarta, también en el camino de error.

El adaptador no habilita terminación automática por timeout: timeout marca
incertidumbre y espera evidencia. La demostración local independiente de IGNITION
puede habilitar SIGTERM por timeout mediante política confiable explícita persistida
en la misma SQLite; no es una concesión derivada del número recibido en JSON.
Si se ignora una terminación permitida, el supervisor conserva su handle, drena y
espera la salida natural; no afirma salida anticipada. Esto puede esperar sin límite.
Un proceso local ininterrumpible o que no salga requiere intervención posterior
separada: no hay recuperación administrativa destructiva implementada.

## Resultado y publicación atómica

El supervisor entrega evidencia local ligada a su hijo; no se acepta evidencia de
un mensaje del orquestador. La autoridad verifica esa identidad, salida y contenido
exacto antes de decidir. Los controles offline simulate_result/simulate_evidence
están deshabilitados en ExecutionLedger. Fallo de proceso, timeout o identidad dudosa
no se transforman en éxito contractual. El backend local puede informar FAILED o
TIMED_OUT como evidencia propia; no se añaden esos estados al contrato de tarea.

`finish` guarda evidencia, compara generation vigente, aplica la decisión de resultado
y registra bytes/hash/ref de artefacto **en una sola transacción**. La aceptación
reutiliza `_accept_result`, la misma lógica del simulador. Si gana la revocación,
el resultado tardío queda segregado sin publicar artefacto ni alterar el intento
nuevo. Si gana publicación, se conserva ese terminal histórico; avanzar generation
no lo reescribe. Cancelación aceptada antes de una finalización de orden incierto
impide declarar éxito/cancelación sin prueba suficiente.

`published_result(session_id)` es una lectura local del canal de resultados de
laboratorio: devuelve únicamente una publicación aún vigente, con la tupla y
result_id/outcome/artifact_ref/artifact_sha256 normativos. No es una quinta operación
wire ni un transporte autenticado implementado. Tras revocación no devuelve el
resultado viejo como vigente. `artifact(ref)` recupera una copia de bytes verificados
conservados en SQLite; no vuelve a leer el archivo mutable del workspace. La API no
ofrece sobreescritura de esa referencia. Perder o alterar ese snapshot bloquea la
lectura/operación; no se reconstruye desde un workspace potencialmente sustituido.
Los artefactos históricos se conservan sin volver a publicarse como actuales.

## Evidencia de pruebas

- HANDSHAKE: **36** pruebas originales, sin cambiar sus fixtures ni tests.
- IGNITION: **23** pruebas: las 19 anteriores adaptadas al almacenamiento/separación
  de conocimiento y 4 adicionales de no señales, resistencia a SIGTERM y autoridad única.
- FORGE LINK: **15** pruebas de ejecución contractual real, hash, snapshot resistente
  a sustitución del archivo, permisos/perfiles/tuplas inválidos, procesos duplicados,
  reinicio, timeout, cancelación, fencing y fallos de publicación.

Se ensayan carreras reales y las seis ordenaciones de cancelación/revocación/
publicación con un proceso que realmente ejecutó y terminó. Una barrera exclusiva de
test detiene la publicación, no fabrica un resultado exitoso. También se ensayan
caídas antes/después del commit final y reintentos sin segundo proceso.

Una prueba reinicia el intérprete con exec mientras su hijo real está vivo. El nuevo
intérprete sólo reconcilia UNKNOWN y reintenta el ACK, sin adoptar ni relanzar la tarea.
Para no dejar un huérfano en el laboratorio, el helper de test conserva la relación
padre-hijo a través de exec y hace waitpid de ese hijo al terminar, sin señales; esto
es teardown de la prueba y **no** una función del adaptador. Perder los pipes durante
el reinicio puede hacer fallar al payload; no se infiere éxito de ese caso.

## Límites y compatibilidad

El usuario local, intérprete, código instalado, directorio temporal y almacenamiento
son confiables. No se acredita aislamiento frente al mismo UID, permisos de archivos
hostiles, manipulación coherente de SQLite, snapshots atrasados, corte de energía,
escalabilidad ni autoridad DMAOL distribuida. No hay contenedor, sandbox, límites de
RAM/CPU ni barrera de red; el programa fijo simplemente no usa red. El root se rechaza.
No existe exactly-once de efectos externos ni publicación en destinos externos.

Los ledgers HANDSHAKE offline conservan su formato y fachada. Los ledgers IGNITION
3D-A de autoridad separada **se rechazan**, sin migración automática ni copia de
generaciones; el laboratorio nuevo se inicializa explícitamente. Una raíz nueva no
es recuperación de un ledger perdido. No se modificó un ledger previo existente.

No se implementa un servicio de recolección de procesos huérfanos. La caída completa
del propietario puede dejar vivo al hijo fijo; recover no lo mata ni considera su
artefacto suelto prueba suficiente. Cerrar el laboratorio es una eliminación explícita
de sus datos temporales: el llamador debe esperar todos los propietarios/reaperturas
antes de limpiar. No se borran workspaces automáticamente por timeout/cancelación.

La publicación aquí significa el registro transaccional local de referencia y bytes;
no una entrega autenticada a DMAOL. La aprobación y autoridad son confiables **locales**,
no integración externa demostrada. Esta etapa no autoriza conectar agentes, desplegar,
comenzar otras fases ni hacer commit/push.
