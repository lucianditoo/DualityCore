# IRONCLAD Fase 2 — preparación de L00 y L01

Estado: no operativo y no validado. Este documento no autoriza ejecución.
Sólo se crearon archivos nuevos de desarrollo. No se ejecutó el harness,
recolector, fixtures, pruebas unitarias ni integración. No se creó una VM ni un
contenedor, no se conectó a Docker y no hubo cambios de servicios, producción,
credenciales, sistema, commits, push, merges o workspace de Antigravity.

## Baseline y material preparado

Commit: ccc54e6f25f37f3541d52d14a8fe295beb95e8e7.
Executor SHA-256:
00ea3fb806afc56b80f45a34c72fb252a6345ef71ffad54102807d9917a75ce0.
Suite SHA-256:
6ab3f38ab58c6797663a79c1abfe431e4efafd24eec379ef9646ef3052aa727d.
Las 122 pruebas exitosas son evidencia histórica, no una ejecución de esta fase.

lab/ironclad/baseline.json fija esos valores; artifacts.lock.json identifica
archivos de código/fixtures y baseline. El lock no se autorreferencia. Los ejemplos
de manifiesto/autorización no se incluyen en ese lock: su versión operativa se
vincula por hash exacto en una autorización del operador. No hay firma ni secreto.
Cambiar cualquier código obliga a generar/revisar nuevo lock y nueva autorización;
no se permite sustituir el Executor ni la suite de baseline.

## Frontera del laboratorio futuro

VM independiente con kernel y daemon exclusivos, preferentemente fuera del host
actual. Sin carpetas compartidas, sockets del host, mounts remotos, red externa,
credenciales o recursos de producción. No basta un nombre distinto o DOCKER_HOST.

Preaprovisionamiento por el operador, FUERA de la autorización actual:

- Imagen de VM y herramientas revisadas, sin instalaciones en el host actual.
- /srv/ironclad/bundle con copia del artefacto y sin escritura por grupo/otros.
- /srv/ironclad/control con manifest.json y authorization.json, 0600 y dueño EUID.
- /srv/ironclad/state, runtime y evidence: directorios distintos, 0700, dueño EUID,
  ancestros root/EUID no escribibles por grupo/otros y sin symlinks. Raíz de evidencia
  preferentemente en volumen distinto al estado para futuros ensayos de disco.
- Sólo interfaz lo. Nada de conexión a producción para «comprobar» el aislamiento.
- /usr/bin/python3 y soporte pidfd disponibles; permisos para leer identidad DMI.
  Si una lectura está prohibida, se rechaza; no se relaja ni se solicita sudo.
- Obtener por procedimiento del operador UUID de VM, boot ID, EUID, namespaces,
  device/inode de raíz y hash exacto de mountinfo. No se recolectan ahora.

Las rutas de laboratorio son sólo una propuesta dentro de la VM futura. No se
crearon /srv ni /opt ni se modificó configuración del sistema actual.
L00/L01 no necesitan instalar nada en /opt/duality-core ni copiar sus archivos.

## Manifiesto y autorización

manifest.example.json está disabled, VM no identificada, Docker disabled y sin
contenedores autorizados. run_id propuesto:
6f1b4c82d9074a35a0e981c6f4d23b70.
Cambiar run_id requiere nuevo hash/autorización y actualizar los comandos exactos.

La versión operativa debe completar identidades reales del guest y conservar el
baseline. Contiene daemon_id, endpoint, socket device/inode/UID y data_root como
pins del daemon previsto. Son NO VERIFICADOS mientras no haya cualificación Docker.
Los campos null no son una identidad válida del daemon. No autorizan conectarse.

authorization.example.json está inactive, sin scopes, fechas o atestaciones.
Un operador debe emitir una copia privada, con UUID no nulo, issued/expires en Unix
UTC (ventana <=3600 s), hashes del manifiesto y lock, VM/boot exactos y atestaciones
externas de independencia, ausencia de acceso productivo, sin mounts del host y
recursos exclusivos. Es un registro de decisión, no una credencial ni prueba de
hipervisor. Un atacante con el mismo UID/root está fuera de esa frontera.

La aprobación inicial debe conceder únicamente L00. Para L01 se revisa primero el
summary de L00, se calcula su SHA-256 y se emite otra autorización con L01_EOF y/o
L01_TIMEOUT y l00_result_sha256. No modificar el manifiesto entre ambos: cambia
su hash y obliga a repetir el preflight. Una autorización L00 no habilita fixtures.

## L00: preflight local, sin Docker

Precondiciones: VM y rutas preaprovisionadas; revisión externa de aislamiento;
identidades completadas; autorización L00 vigente; artefactos revisados.

Procedimiento propuesto:

1. Invocar el comando L00 especificado abajo dentro de la VM identificada.
2. Validar gates y hashes antes de observaciones/evidencia.
3. Comparar DMI/boot/EUID/namespaces/raíz/mountinfo con pins; verificar sólo lo,
   tipos de filesystem permitidos, ancestros y raíces privadas sin alias.
4. Rechazar si hay latch de parada o experimento simultáneo. Crear únicamente
   directorio de evidencias, lock y archivos privados.
5. Conservar events.jsonl y summary.json; revisión independiente por operador.

Resultado aceptable limitado: PROCESS_PREFLIGHT_ONLY. Docker se informa como
NOT_CONTACTED_NOT_VERIFIED y docker_execution_allowed=false. Exit 3 significa
preflight parcial; impide tratar el comando como aprobación global. No equivale a PASS
completo de L00 ni a laboratorio validado para Docker/Executor.

Criterios: cero conexiones/socket Docker, subprocesses o cambios fuera de evidencia;
hashes y observaciones coincidentes; atestación externa revisada; evidencia completa.
Un fallo debe producir rechazo, no un «skip» exitoso ni defaults de identidad.

## L01 EOF: primer proceso real propuesto

Precondiciones: L00 aprobado para procesos, mismo manifiesto/VM/boot/bundle,
autorización L01_EOF ligada al hash de su resumen; sin latch.

Procedimiento: extraer literal del launcher por AST y comprobar hash; abrir pipe;
crear un único hijo Python con ese literal y target canario fijo; abrir pidfd y
registrar PID/ticks; cerrar escritor sin enviar permiso; observar salida acotada.
No se importa ni ejecuta el módulo Executor; no hay create/destroy ni Docker.

Evidencias: evento de inicio, PID/ticks/boot, apertura pidfd, cierre sin permiso,
exit code, salida acotada, tiempo y resumen. El target, si se ejecuta, imprime
IRONCLAD_UNEXPECTED_EXEC y sale 126; un fallo así nunca debe aprobarse como 125.

Criterio de aprobación: exit 125, stdout/stderr vacíos, cero bytes de permiso,
identidad/pidfd observables, cierre del escritor antes de 4 s y sin deadline agotado.
Si el padre tarda en registrar evidencia y se aproxima al timeout de 5 s, EOF
queda inconcluso: no atribuir a EOF una salida causada por timeout. L01_EOF_COMPONENT_PASS sólo
aprueba ese escenario. Para demostrar ausencia de exec con evidencia de kernel
más fuerte se necesitará tracing externo autorizado; no está implementado aquí.

## L01 timeout: escenario separado

Mantener escritor abierto, no enviar permiso y observar el timeout original de
5000 ms del launcher. Mismas precondiciones con scope L01_TIMEOUT. Esperar salida
125, sin marcador/salida, al menos 5 s desde el inicio y antes del deadline global
configurado (9 s en el ejemplo). Registrar carga/planificación para interpretar
latencia. No confundir ese tiempo con un timeout de pidfd o del Executor completo.

El deadline del bucle NO acota Popen, lecturas, fsync ni otras llamadas bloqueantes.
L01 queda deshabilitado en Fase 2C hasta preparar supervisión externa independiente.
No relajar límites automáticamente para obtener un PASS.

## Recolector, abortos y conservación

No se ejecutó el recolector. Está preparado para archivos exclusivos 0600,
directorios 0700, eventos acotados con hash encadenado, fsync, resumen ligado a
manifiesto/autorización/bundle y lock cooperativo no bloqueante. No usa snapshots
recursivos, env dumps, ps global ni archivos arbitrarios. No captura credenciales.

Un error, salida inesperada, evidencia incompleta, exceso de salida o hijo vivo
fija .IRONCLAD_STOP cuando puede persistirlo. No se borra ese latch automáticamente.
Si falla almacenamiento no se puede garantizar persistirlo: abortar y conservar
la VM para revisión externa. No hay limpieza automática, unlink de recursos,
terminate/kill ni señalización por PID numérico. Los descriptores se cierran;
cerrar el pipe permite EOF. Un hijo que siga vivo requiere autorización separada.

El resultado debe clasificarse como componente aprobado, fallo, inconcluso o no
verificado. No hay estado «laboratorio operativo» o «Executor seguro».
La ausencia de procesos ajenos dañados no prueba que la política pidfd sea correcta.

## Comandos exactos PROPUESTOS, NO EJECUTADOS

Sólo dentro de una VM posteriormente identificada y autorizada. Estos comandos
no provisionan directorios, completan identidades ni conceden permiso por sí solos.
Los ejemplos del repositorio no pueden usarse directamente para habilitar ejecución.
El comando limpia el entorno y usa Python aislado; no hereda PYTHONPATH, HOME,
contextos Docker ni configuración de credenciales del llamador.

Primera solicitud, únicamente L00:

```sh
/usr/bin/env -i PATH=/usr/bin:/bin LANG=C.UTF-8 IRONCLAD_ENABLE_EXECUTION=OPERATOR_APPROVED /usr/bin/python3 -I -B /srv/ironclad/bundle/lab/ironclad/harness.py --case L00 --manifest /srv/ironclad/control/manifest.json --authorization /srv/ironclad/control/authorization.json --execute
```

Después de revisar L00 y emitir autorización específica, solicitud separada EOF:

```sh
/usr/bin/env -i PATH=/usr/bin:/bin LANG=C.UTF-8 IRONCLAD_ENABLE_EXECUTION=OPERATOR_APPROVED /usr/bin/python3 -I -B /srv/ironclad/bundle/lab/ironclad/harness.py --case L01 --scenario eof --manifest /srv/ironclad/control/manifest.json --authorization /srv/ironclad/control/authorization.json --preflight-result /srv/ironclad/evidence/6f1b4c82d9074a35a0e981c6f4d23b70-L00/summary.json --execute
```

Solicitud adicional, no implícita en EOF:

```sh
/usr/bin/env -i PATH=/usr/bin:/bin LANG=C.UTF-8 IRONCLAD_ENABLE_EXECUTION=OPERATOR_APPROVED /usr/bin/python3 -I -B /srv/ironclad/bundle/lab/ironclad/harness.py --case L01 --scenario timeout --manifest /srv/ironclad/control/manifest.json --authorization /srv/ironclad/control/authorization.json --preflight-result /srv/ironclad/evidence/6f1b4c82d9074a35a0e981c6f4d23b70-L00/summary.json --execute
```

No se solicita ejecutar Docker, fixtures reservados, pruebas posteriores, cambios
de sistema, limpieza, reinicios, commits o despliegue. Antes de ejecutar cualquier
comando se necesita aprobación específica del operador y verificar su destino VM.

## Riesgos pendientes

Código nuevo sin ejecutar ni validación sintáctica por intérprete; puede contener
fallos. Los guards dependen de filesystem local confiable, permisos y operador;
no detectan todas las formas de bind mount/hipervisor compartido. Hashes no firman
artefactos ni protegen frente a root. El reloj de expiración debe ser coherente en
VM offline. DMI/namespace/boot y lectura de /proc pueden fallar: se rechaza.

No se verifica identidad Docker, launcher contra power loss, PID reuse, permisos
interusuario, descendientes, Bridge ni journals reales del Executor. Socket fixture
0600 es deliberadamente privado, no una prueba de acceso del agente. No se declara
compatibilidad del gateway sintético con producción. La finalización sin señales
puede dejar procesos en observación, por lo que el presupuesto y supervisión externa
son necesarios. La versión v3 y sus cinco observaciones de continuidad no se
modificaron en esta fase.


## Fase 2C — estado vigente; prevalece sobre las propuestas anteriores

Sólo L00 puede habilitarse, siempre en una VM descartable independiente con nueva
aprobación. SCOPES acepta exclusivamente L00; L01 también rechaza llamadas directas.
Los comandos L01 anteriores son propuestas históricas NO EJECUTABLES con esta versión.
Los fixtures activos se retiraron: no crean sockets, ignoran señales ni emiten heartbeat.
Su restitución requiere otra autorización, lock global y supervisión independiente.
El canario reservado conserva salida 126 con una única escritura no bloqueante.

H01: bajo flock exclusivo se crea .IRONCLAD_IN_PROGRESS mediante O_EXCL; se fsync
el archivo y la raíz ANTES del directorio del caso. Cualquier marca preexistente,
incluso vacía o un symlink, impide continuar. La marca sólo se elimina al salir
normalmente, después de fsync del resumen y su directorio; luego se fsync la raíz.
Excepciones, muerte abrupta o fallos de almacenamiento conservan la marca cuando
el almacenamiento lo permite. No hay borrado ni reparación automática de estados
incompletos. El operador debe preservar la VM/evidencia y autorizar por separado
cualquier desbloqueo. Una caída posterior a persistir el resumen puede bloquear
conservadoramente la siguiente operación. El flock no sustituye esta marca.

H02/H06: no existe ruta activa de fixtures que evite el lock o resuelva un socket
por pathname. H05: state/runtime/evidence deben ser exactamente las tres rutas
/srv/ironclad/state, /srv/ironclad/runtime y /srv/ironclad/evidence. No se admiten
otras raíces privadas. Los controles por componentes, propietarios y modos siguen
vigentes. EUID/root capaz de alterar el bundle o sus directorios continúa fuera
de la frontera; los checks no constituyen una sandbox frente al propio operador.

H03: L00 no tiene un deadline total garantizado ante I/O bloqueado. Un watchdog
en el mismo intérprete no resolvería fsync o syscalls bloqueantes. La siguiente
prueba L00 requiere observación externa del operador por consola del hipervisor:
si el comando no finaliza en 30 s, clasificar INCONCLUSO, no repetir ni desbloquear,
y preservar la VM para revisión. Ese plazo es un criterio operativo de detección,
NO una promesa de terminación automática. No se autorizan aquí watchdog, señales,
reinicios ni recursos nuevos. L01 sigue bloqueado hasta diseñar supervisión externa
real con presupuesto, identidades y política de intervención autorizada.

H04: se eliminan los modos activos que prometían dos segundos; el canario no usa
print bloqueante. H07: la autorización se revalida dentro del recolector antes de
abrir evidencia y después de adquirir el lock, inmediatamente antes de crear la
marca; ningún check elimina la ventana de planificación ni controla retrocesos del
reloj del guest. L00 requiere reloj coherente, sin cambios durante el ensayo.
H08: código y documentación del laboratorio son 0644, JSON 0600 y directorios de
código 0755; raíces operativas siguen requiriendo 0700. Verificar estos permisos
después de copiar el bundle, sin relajar los guards. H09: entradas CLI exigen -I -B
antes de importaciones no esenciales; el intérprete y su biblioteca de sistema
siguen siendo parte confiable de la imagen. Usar siempre env -i como está indicado.

Validación local autorizada: análisis AST/JSON/hashes y test_harness_unit.py usando
un directorio temporal. El abridor de evidencia se sustituye únicamente en tests;
los guards operativos no se relajan. Los tests de flock y persistencia usan archivos
locales reales, sin hijos, sockets, Docker o identidades de la VM. Simular pérdida
del controlador cerrando descriptores no prueba SIGKILL, power loss ni durabilidad
del disco. Esas garantías quedan pendientes del laboratorio autorizado.

Comando local de pruebas aisladas (no inicia hijos externos):
python3 -I -B lab/ironclad/test_harness_unit.py
