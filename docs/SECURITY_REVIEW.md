# Revisión de seguridad — candidato v3

Fecha: 2026-10-21. Sólo desarrollo. Esta revisión sustituye las garantías excesivas
del primer informe: una suite con mocks no demuestra comportamiento del kernel,
daemon Docker, gateway, Bridge ni systemd. No se ejecutó ninguna operación sobre
infraestructura real. No se leyeron credenciales. La referencia production sigue
sin modificaciones.

## Arquitectura y fronteras

El CLI administrativo admite create, destroy y recover por identificador validado.
No es una API de agentes. Lee únicamente una configuración administrativa privada
config.v3.json, distinta de config.json del ejecutor antiguo. Exige esquema 3,
identificador de deployment y atestación explícita legacy_executor_fenced=true.
La atestación obliga a una decisión operativa; no revoca por sí sola permisos de
root ni demuestra que otro servicio haya sido deshabilitado.

Las raíces de estado durable (/var/lib/duality-core/sessions-v3), runtime
(/run/duality-core/gateways-v3), workspace v3 y workspace legacy deben estar
preaprovisionadas, ser distintas y no contenerse entre sí. Se rechazan symlinks
al recorrerlas y alias de sus inodos raíz. El agente debe tener UID positivo y
diferente del ejecutor. No se permite usar root como agente. El tmpfs de Codex
usa el UID/GID configurado, sin constantes 10001.

Cada sesión conserva su registro privado durable, una identidad de proceso,
instancia aleatoria e ID Docker completo. El runtime no contiene registros y es
lo único que se monta en /gateway. El workspace permanece tras destrucción y
rollback. Los nuevos nombres incluyen lifecycle-v3 + deployment de 32 caracteres:
con el prefijo legacy declarado, ningún SESSION_ID de hasta 40 caracteres del
CLI antiguo puede expresar esos nombres. El namespace no sustituye un fencing
operativo frente a un administrador que cambie el prefijo del ejecutor anterior.

## Resultado de los 14 hallazgos de la segunda auditoría

| N.º / severidad anterior | Tratamiento y evidencia actual | Límite restante |
| --- | --- | --- |
| 1 Alto: boot ID malformado | UUID canónico no nulo en identidad persistida y boot actual; registro con SHA-256; regresión rechaza guiones sin realizar acciones | El digest detecta corrupción, no autentica frente a un propietario malicioso |
| 2 Alto: docker_uncertain opcional | Todos los campos son obligatorios; bool exacto, esquema int exacto; ausencia conserva evidencia y falla antes de Docker | Ningún formato incompleto se adopta automáticamente |
| 3 Alto: ejecutor antiguo sobre v2 | Namespace v3 fuera de la gramática legacy, configuración/runtime/estado/workspace separados, gate de fencing y rechazo de registros v2 | Revocar invocaciones antiguas y validar rollback son tareas del operador; no hay revocación de root desde Python |
| 4 Alto condicional: TOCTOU de ancestros | Recorrido con openat/dir_fd + O_DIRECTORY/O_NOFOLLOW, comprobación fstat de cada componente, escrituras/chown/unlink relativas a fd fijados | Docker y el gateway resuelven rutas propias; se confía en administradores y topología de montajes, no en un root hostil |
| 5 Medio: creación exitosa con gateway muerto | Validación pidfd antes de Docker, doble verificación de gateway y Running/Paused/Restarting/Status después de start y de persistir active | Supervivencia observada, no promesa de vida futura ni salud funcional de Bridge |
| 6 Medio: destrucción previa a validar gateway | Mantiene abierto pidfd verificado, comprueba propiedad Docker antes de primera mutación; mismatch no elimina contenedor | Un timeout posterior puede dejar una operación parcial y reintentable |
| 7 Medio: integridad de datos | SIGTERM de contenedor + wait + inspección de salida; rm sin -f/-v; si start pudo ejecutarse, create no destruye automáticamente recursos | SIGTERM no demuestra checkpoint ni consistencia de la aplicación; requiere contrato de Bridge |
| 8 Medio: bloqueo infinito | LOCK_NB con deadline de 5 s; stdout fuera del lock; pruebas concurrentes y liberación ante excepción | flock es cooperativo/local; I/O bloqueada en kernel no tiene un límite absoluto desde Python |
| 9 Medio: persistencia incompleta | Journal durable separado de /run; intención previa; launcher con permiso posterior a fsync de identidad; rollback cubre permisos de runtime | Caída antes de registro inicial o en ventana spawning conserva cuarentena; fsync y launcher real pendientes de laboratorio |
| 10 Medio: zombie/cmdline vacía | Consulta pidfd antes de cmdline y reconsulta al observar comando inválido o /proc desaparecido; descriptor legible confirma salida | Intercalados reales de exit/reap requieren laboratorio |
| 11 Medio: recover incompleto | Informa archivos inesperados, checksum/esquema inválidos, Docker incierto, flags, estado del proceso/contenedor, runtime/socket ausente | No demuestra muerte de Bridge ni autoriza limpieza automática |
| 12 Medio: UID/GID e infraestructura | Configuración estricta, separación agente/operador, validación descriptor de propietario/modo, tmpfs parametrizado | Chown entre usuarios distintos, privilegios del gateway e integración Bridge siguen pendientes |
| 13 Bajo: esquema permisivo | Tipos exactos, claves exactas, invariantes por fase, UUID, ID hexadecimal y rechazo de JSON duplicado/NaN/Infinity | No hay importación de formatos previos |
| 14 Medio: falsa confianza en mocks | Suite ampliada, modelo Docker con estados, fallos posteriores al efecto, filesystem temporal y concurrencia real de threads sobre flock temporal | No reemplaza ensayos de pidfd, Docker, permisos entre usuarios ni caídas de VM |

Los hallazgos 3, 9, 12 y 14 incluyen controles implementados y condiciones
operativas/laboratorio pendientes; no se declaran íntegramente demostrados por
mocks. Los fallos reproducibles de la auditoría tienen regresiones específicas.

## Garantías del código

- Boot IDs inválidos, metadata ausente/corrupta, campo incierto omitido, symlinks,
  archivos inesperados o identidades discordantes no habilitan limpieza insegura.
- read_private_json limita bytes efectivamente leídos, exige archivo regular,
  dueño efectivo, 0600 y un solo enlace. JSON duplicado y números no finitos fallan.
  El envelope incorpora un SHA-256 obligatorio de serialización canónica.
- write_record valida estado, crea temporal exclusivo 0600, fsync, replace relativo
  a descriptor y fsync de directorio. No usa rutas resueltas nuevamente para la
  escritura. Un fallo de durabilidad conserva evidencia; nunca implica éxito.
- Los componentes de cada ruta administrativa se abren sin seguir symlinks y se
  comprueban por fd. Un ancestro sticky propiedad de root se permite sólo como
  ancestro, no como raíz leaf; sticky impide que otro usuario sustituya el siguiente
  componente que debe pertenecer a root o al ejecutor. El agente no puede ser ese UID.
- La identidad del launcher se sincroniza antes de habilitar exec del gateway.
  exec mantiene PID/ticks; el gateway debe presentar después el comando completo
  esperado. EOF, permiso incorrecto o timeout de la compuerta no ejecutan gateway.
- Un pidfd verificado se mantiene abierto durante el cierre del contenedor y
  señalización del gateway. No se adopta os.kill(pid). SIGTERM se envía por pidfd;
  salida por POLLIN, ESRCH del destinatario fijado o boot anterior canónico.
- En rollback antes de start, el único proceso señalado por Popen es su hijo
  directo no recolectado: no un PID adoptado desde metadata. Se espera su salida.
- Docker se descubre por nombre o ID persistido; se inspecciona siempre el ID
  completo, nombre y etiquetas incluyendo esquema/instancia. Un ID renombrado,
  sustituido o una instancia distinta bloquea la limpieza. rm nunca usa nombre,
  -f ni -v. Se vuelven a verificar propiedad y estado antes de retirarlo.
- start_attempted se persiste antes de enviar start. Desde ese momento, cualquier
  fallo de create conserva recursos y devuelve CREATE_INCOMPLETE_REVIEW_REQUIRED.
  Antes de start, un resultado create incierto sin ID visible conserva journal.
- recover no crea lock/raíces ni escribe registros, envía señales o cambia Docker.
  Requiere un lock preexistente y devuelve evidencia bajo exclusión cooperativa.

## Tercera auditoría del cambio

Se revisaron de nuevo fases, excepciones y reintentos después de ampliar las
pruebas. Esa revisión detectó y corrigió: reintento de registro closed con
start_attempted histórico; colisión por prefijo legacy distinto; Config Docker
malformado; timeout Docker sin journal que escapaba al diagnóstico; ausencia de
socket/runtime no informada; alias de raíces; configuración compartida con legacy;
liberación del gateway antes de persistir identidad y fugas de descriptor en
errores de apertura de runtime o consulta de pidfd tras desaparecer /proc. Cada caso cuenta con
regresión o comprobación de política.

No se encontró en esa revisión una vía confirmada para señalar un PID reutilizado
o eliminar un contenedor de otra instancia bajo los supuestos de confianza.
No es una prueba formal ni una afirmación sobre la infraestructura actual.

## Riesgos residuales y severidad

- Alto operacional hasta verificar fencing: un root que cambie configuración,
  prefijos, montajes o ejecute Docker directamente puede sobrepasar el namespace.
  Ningún dato booleano local revoca esos privilegios. Bloquea despliegue real.
- Medio: launcher/pidfd/exec/ticks, Docker CLI y permisos interusuario están
  simulados. Su semántica real y el comportamiento de Bridge deben aprobarse en lab.
- Medio: gateway hereda la identidad del ejecutor. No se conoce su implementación
  ni política de capacidades; debe considerarse parte confiable del control plane.
  No se afirma aislamiento frente a un gateway comprometido con privilegios del host.
- Medio: no hay cgroup por sesión ni prueba de muerte/fencing de Bridge. El cierre
  confirma el proceso gateway, no posibles descendientes que éste haya creado.
- Medio: SIGTERM permite cierre cooperativo, pero una aplicación puede ignorarlo,
  salir sin flush o dejar archivos inconsistentes. En timeout no hay SIGKILL ni rm.
- Medio: fsync puede fallar o bloquear; la durabilidad efectiva depende del
  filesystem, almacenamiento y supervisor. No hay límite global contra I/O en
  estado ininterrumpible. No usar filesystem de red sin validación adicional.
- Medio: registro perdido/corrupto, spawning sin identidad, temporales de una
  caída y directorio durable vacío requieren revisión. No se reconstruye propiedad
  únicamente por nombre y no se promete recuperación automática en todos los crashes.
- Medio: la resolución externa de bind mounts y ejecutables presupone que nadie
  con privilegios de administrador cambia ancestros o montajes durante la operación.
  Se rechazan aliases de raíces; no se detecta toda topología de bind mounts anidados.
- Bajo: lectura diagnóstica puede actualizar atime; «no destructivo» significa sin
  cambios de contenido/estado de recursos, no un snapshot inmutable ni sin lecturas.
- Bajo: la vida futura de una sesión y salud de la aplicación no quedan garantizadas
  por inspecciones puntuales. No hay handshake autenticado de readiness disponible.

## Evidencia de pruebas

Suite actual: 122 pruebas, con subcasos adicionales de formatos, tipos y fases.
Comando: PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -s tests -v.
Se bloquean por defecto subprocess.run/Popen, pidfd_open, pidfd_send_signal y
os.kill; sólo se habilitan mocks explícitos. No se crea ningún gateway ni socket.
El launcher se evalúa con execv/read/poll simulados; no lanza procesos reales.
Filesystem, hardlinks/symlinks y flock de threads se ejercitan sólo en /tmp privado.

Este sandbox presenta / y /tmp con UID mapeado 65534. El fixture normaliza sólo
los dos inodos ya existentes para poder comprobar las rutas temporales; el
candidato de producción mantiene la exigencia de root/euid. Eso tampoco prueba
propiedad del host real. Los tests de filesystem usan un único usuario; la
validación CLI rechaza ese UID como agente y la separación interusuario queda
expresamente pendiente. Ningún test aprobatorio debe presentarse como evidencia
experimental de pidfd real, Docker, systemd, ACL complejas o durabilidad tras reboot.
