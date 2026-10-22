# IRONCLAD Fase 2: artefactos preparados, no ejecutados

Estado: PREPARACIÓN / NO OPERATIVO / NO VALIDADO.
Baseline inmutable: ccc54e6f25f37f3541d52d14a8fe295beb95e8e7.
No se modifican executor/sessions.py ni tests/test_sessions.py.

Esta carpeta no provisiona una VM ni instala nada. No contiene cliente Docker,
proxy API, comandos de contenedores, systemd, cambios de sistema ni limpieza.
No se ejecutó harness, recolector, fixture o prueba durante la preparación.

## Arquitectura

harness.py coordina sólo L00 (preflight local de procesos) y L01 (EOF/timeout del
launcher). guard.py verifica autorización, hashes, identidad local y raíces.
evidence.py prepara JSONL acotado, hashes encadenados, resumen y latch de parada.
fixtures/process_fixture.py contiene el canario de exec y modos sintéticos
reservados. fixtures/gateway_fixture.py es un listener Unix sintético reservado,
sin TCP, upstream, credenciales ni compatibilidad declarada con Bridge.
fixtures/launcher_contract.json identifica el literal del launcher original.
El harness extrae ese literal con AST, sin importar/ejecutar el Executor.

## Deshabilitado por defecto

Todas estas condiciones son necesarias simultáneamente:

- --execute y variable IRONCLAD_ENABLE_EXECUTION=OPERATOR_APPROVED.
- Copia revisada en /srv/ironclad/bundle, dentro de VM independiente autorizada.
  Se rechaza ejecutar desde este workspace, incluso con el flag y la variable.
- Control exclusivamente en /srv/ironclad/control/manifest.json y authorization.json.
- execution_enabled=true, autorización activa, scope explícito y vigencia máxima
  de una hora. Los ejemplos tienen ejecución deshabilitada y scopes vacíos.
- Hash del manifiesto y artifacts.lock.json aprobados; hashes del Executor,
  suite y código/fixtures idénticos al bundle aprobado.
- UUID DMI, boot ID, EUID, namespaces, filesystem y mountinfo coincidentes.
- Sólo interfaz lo, sin mounts de filesystem desconocido/compartido/contenedor,
  ancestros no escribibles por grupo/otros ni symlinks, raíces privadas distintas.
- Sin .IRONCLAD_STOP y sin otro experimento reteniendo el lock de evidencias ni marca IN_PROGRESS.
- L01 exige además hash aprobado del summary.json de L00 del mismo manifiesto,
  bundle, VM y boot. Permiso L00 no concede L01 ni los scopes de fixtures.

No existen defaults de respaldo, opción --force, autodetección permisiva de
identidades, normalización de UID del sandbox ni escalado de privilegios.
Un operador/root puede falsificar un archivo de autorización o el guest: estos
checks no son una firma, un mecanismo de autenticación ni prueba criptográfica
de hipervisor. La independencia exige atestación externa revisada del operador.

## Alcance de L00

No se abre ni conecta el socket Docker. Daemon ID y socket/device/inode/owner son
campos previstos para pins externos; sus valores no se comprueban por consulta.
El único resultado positivo local es PROCESS_PREFLIGHT_ONLY, con
NOT_CONTACTED_NOT_VERIFIED para Docker. Devuelve exit 3 (parcial), no éxito
global. NO aprueba L00 completo para Docker.
Una cualificación del daemon requiere otra fase y autorización específica.
No se habilita Docker aunque alguien cambie docker.execution_enabled a true:
la política actual lo rechaza. IDs y nombres de contenedores autorizados: vacíos.

## Alcance de L01

Se crea, en una futura ejecución autorizada, exactamente un hijo directo Python
con el literal original del launcher. Nunca se escribe el permiso b'1'. El target
es un canario fijo que, si fuese ejecutado, imprime IRONCLAD_UNEXPECTED_EXEC y
sale 126. El resultado esperado del launcher es 125, sin salida ni exec del target.
EOF cierra el escritor antes de 4 s para no confundirlo con el timeout de 5 s;
si no se alcanza esa frontera el resultado no aprueba. Timeout conserva el
escritor abierto sin escribir. No se inicia
sesión del Executor, gateway real, socket, contenedor ni proceso externo adicional.

Se intenta abrir pidfd y registrar identidad del hijo para observación; esto NO
prueba protección frente a PID reuse. No hay kill, terminate ni SIGKILL automático.
En timeout/anomalía se conserva evidencia y se fija .IRONCLAD_STOP. Si queda un
hijo vivo, la VM queda para revisión: no hay borrado ni señalización automática.
La limpieza manual de ese hijo requiere alcance separado y prueba de identidad.

## Evidencias

Raíz preaprovisionada euid-owned 0700. Un directorio exclusivo run_id-scope evita
sobrescritura. events.jsonl y summary.json son 0600, con fsync y límites. El hash
encadenado detecta corrupción accidental; no impide manipulación por el dueño.
Los locks/archivos de evidencia se crean sólo DESPUÉS de autorización y preflight.
Si no se puede crear/persistir evidencia se aborta, sin declarar un resultado válido.
Errores tras crear un hijo pueden dejar un resultado incompleto: preservar la VM.

No se captura entorno, ps global, archivos arbitrarios, memoria, credenciales,
configs de producción ni logs de servicios. Los únicos datos del kernel son
identidad local, mountinfo e interfaces; de procesos se registra sólo el hijo propio.
No hay CLI independiente del recolector ni ejecución al importar sus definiciones.

## Fixtures reservados

Heartbeat, ignore-term y el gateway activo están bloqueados en Fase 2C. El gateway es Unix local,
con directorio exclusivo, socket 0600 y una conexión de prueba acotada. Son fixtures
negativos/sintéticos; no comprueban acceso de otro UID ni son el gateway productivo.
Requieren FIXTURE_PROCESS/FIXTURE_GATEWAY y modos explícitos, no incluidos en los
comandos solicitados para L00/L01. Nunca leen claves ni archivos OAuth. No borran
el socket ni directorios. Su uso necesita una autorización futura independiente.

## Revisión pendiente

Durante Fase 2 no se ejecutaron estos artefactos. Fase 2C permite únicamente validación sintáctica y pruebas unitarias locales aisladas.
Se efectuó revisión de texto y hashes de preparación, no una suite de pruebas.
Antes de autorizar procesos reales, revisar el código, esquema operativo, errores,
limpieza de descriptores, latch, límites y generación del bundle. El harness puede
contener defectos: preparar código no demuestra su corrección.

Procedimientos y comandos propuestos: docs/IRONCLAD_PHASE2_L00_L01.md.


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
