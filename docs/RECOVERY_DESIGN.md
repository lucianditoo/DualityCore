# Recuperación conservadora y registro durable v3

## Espacios separados y contrato

Config nueva: /opt/duality-core/executor/config.v3.json; la anterior no se modifica.
STATE_ROOT: /var/lib/duality-core/sessions-v3 (privado y durable).
GATEWAY_ROOT: /run/duality-core/gateways-v3 (runtime volátil).
Workspace: raíz nueva configurada, distinta y sin solapamiento con legacy,
STATE_ROOT y GATEWAY_ROOT. Todas deben preexistir con propietario correcto.

Nombre Docker: <prefix>-lifecycle-v3-<deployment_id de 32 hex>-<session_id>.
El sufijo mínimo excede la gramática completa de SESSION_ID del ejecutor antiguo.
La política valida esto también respecto del prefijo legacy declarado. Nunca se
reconfigura legacy para que su prefijo alcance este namespace.

Registro STATE_ROOT/<session_id>/session.json, privado 0600, un solo enlace:
envelope con claves exactas record y sha256. record contiene obligatoriamente:
schema_version=3, deployment_id, session_id, name, instance, phase, gateway,
gateway_exit_confirmed, container_id, docker_uncertain y start_attempted.
Los bool no aceptan enteros y los int no aceptan float/bool. Identidades tienen
PID positivo acotado, UUID canónico no nulo de boot y ticks positivos acotados.
SHA-256 detecta corrupción accidental; no es firma ni defensa contra el dueño.

## Estados y fronteras de crash

| Fase | Evidencia y respuesta |
| --- | --- |
| prepared | Sin gateway/contendor; salida confirmada por no haberlo lanzado |
| spawning | Intención durable previa a Popen, sin identidad: cuarentena |
| gateway | Identidad durable; sólo entonces se entrega permiso al launcher para exec |
| docker_uncertain=true | Pudo enviarse create, pero todavía no hay ID confirmado; no asumir cancelación por timeout |
| container | ID completo durable; todavía no se intentó start |
| starting | start_attempted=true durable antes de enviar start; desde aquí no hay rollback destructivo automático |
| active | Se observaron gateway vivo y contenedor running antes y después del commit |
| stopping | Limpieza explícita en curso, identidad original conservada, progreso reintentable |
| closed | Contenedor ausente y gateway con salida confirmada; sólo resta retirar archivos/directorios |

La compuerta usa pipe anónimo heredado exclusivamente por un launcher mínimo.
Antes de autorizar exec se capturan y sincronizan PID/boot/ticks. El launcher sale
sin ejecutar el gateway ante EOF (incluida muerte del ejecutor), timeout de 5 s o
permiso inválido. Cierra el descriptor antes de exec. No lee credenciales. Las
pruebas verifican el protocolo con mocks; secuencia de exec, mismo PID/ticks y
crash real son condiciones de laboratorio, no garantías experimentalmente medidas.

Si el ejecutor muere antes de persistir identidad, spawning sigue en cuarentena:
el launcher no fue autorizado a ejecutar gateway, pero no se afirma conocer su
PID ni haber observado su salida. Si muere después de autorizar, la identidad ya
estaba persistida. No se borra runtime mientras quede una salida sin confirmar.

El journal sobrevive la pérdida de /run. Tras reboot, una identidad válida de otro
boot demuestra que el proceso anterior no sigue vivo, sin señalar al PID actual.
El contenedor con restart=no puede seguir existiendo detenido; se verifica su ID
y todas sus etiquetas antes de retirarlo. Restauraciones de snapshots, clonación
de host y filesystem durable necesitan revisión operativa independiente.

## Creación y fallos parciales

Se comprueban config, namespace, raíces y existencia antes de preparar workspace.
Las operaciones de permisos de runtime están dentro del manejo de errores.
Workspace nunca se borra, incluso si su preparación falla: reparación de dueño o
permisos de uno recién creado puede requerir intervención, sin arriesgar archivos.

Antes de Docker create se sincroniza docker_uncertain=true. Si hay timeout y luego
aparece un ID con instancia/etiquetas correctas, la limpieza puede adoptarlo y
persistir ese ID. Si no aparece, conserva evidencia; el daemon podría crearlo tarde.
Un ID diferente o etiquetas discordantes impide actuar.

Se persiste start_attempted antes de start. Cualquier error desde esa frontera,
incluso timeout o fallo de escritura de active, devuelve
CREATE_INCOMPLETE_REVIEW_REQUIRED y preserva gateway, contenedor y workspace.
El coordinador no debe tratarlo como inexistencia ni emitir un create nuevo a
ciegas. recover permite diagnosticar y una decisión explícita autoriza destroy.
Antes de start, el rollback espera al hijo Popen; si la salida no se confirma,
conserva archivos y devuelve CREATE_ROLLBACK_INCOMPLETE.

## Destrucción explícita

1. Adquirir flock local global con plazo de 5 s. No se borra ni reemplaza su archivo.
2. Abrir directorios por descriptor; validar envelope, esquema, fase y contenido.
   Un registro corrupto, incompleto, legacy o spawning sin identidad detiene todo.
3. Abrir pidfd y comprobar identidad/comando completo del gateway, o confirmar
   salida por descriptor/ausencia/boot anterior. Mantener abierto el pidfd.
4. Comprobar ID, nombre y etiquetas Docker. Sólo después persistir stopping.
   Si create sigue incierto y no hay ID visible, detenerse sin retirar evidencia.
5. Si el contenedor corre, enviar SIGTERM por ID y esperar con docker wait acotado.
   Verificar de nuevo propiedad y estado detenido. No usar stop con escalado ni
   kill sin --signal=TERM. Un timeout conserva gateway y recursos para reintento.
6. Terminar el gateway por el pidfd ya verificado y esperar salida confirmada.
   POLLERR/POLLNVAL no son salida. Cmdline vacía de un proceso salido se resuelve
   consultando pidfd, sin rechazar por defecto un zombie ya terminado.
7. Persistir gateway_exit_confirmed; volver a verificar estado Docker y retirar
   mediante rm ID sin -f ni -v. Confirmar ausencia. Un rm incierto conserva journal.
8. Persistir closed. Revisar otra vez contenido; retirar socket/runtime y luego
   registro/directorio de estado. No se recorren ni borran workspaces.

Se toleran reintentos tras cierre confirmado, rm cuya respuesta se perdió o registro
closed. Puede haber acciones parciales: un contenedor ya detenido cuando el gateway
no termina, por ejemplo. No se promete atomicidad entre Docker y procesos.

Una caída tras unlink de session.json y antes de rmdir puede dejar un directorio
vacío. Sin evidencia suficiente no se infiere salida del gateway: conserva el
residuo para revisión. Temporales de un crash también quedan preservados. Se
prefiere un residuo administrativo a una limpieza que pierda identidad.

## Diagnóstico recover

Usa raíces y lock existentes; no los crea. No envía señales, no ejecuta Docker
mutante y no escribe registros. Emite JSON con status (absent, quarantine u
operator-review), phase, docker_uncertain, start_attempted, gateway, container,
bridge_liveness=unknown e issues. Informa archivos inesperados, errores de
integridad/esquema, Docker indisponible/incierto, procesos no verificables y
runtime/socket ausentes. Las lecturas pueden actualizar atime.

operator-review no significa «huérfana comprobada», «sana» ni «autorizada para
borrar». La muerte de Bridge debe acreditarse por procedimientos externos. No
hay adopción automática ni barrido destructivo al arranque. Destroy es una
instrucción administrativa explícita sobre una sesión específica.

## Pendiente de laboratorio y extensión posterior

Validar launcher y EOF tras SIGKILL, pidfd y PID reuse/zombie reales, paso a través
de exec, restricciones /proc, permisos entre usuarios y cierre cooperativo de
Bridge. No está implementado fencing distribuido ni supervisión por cgroup de
descendientes; esos controles requieren diseño adicional. La configuración
legacy_executor_fenced es atestación de un operador, no prueba de muerte de Bridge.
