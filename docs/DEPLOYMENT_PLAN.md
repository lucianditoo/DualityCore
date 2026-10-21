# Plan de laboratorio, migración y rollback — v3

Estado: propuesta; no ejecutada. No autoriza despliegue ni cambios de servicios,
producción, Docker real, credenciales o permisos del host. La tarea actual sólo
modificó el workspace de desarrollo y ejecutó pruebas aisladas.

## Puerta para laboratorio descartable

El candidato puede pasar a un laboratorio independiente para comprobar supuestos
que los mocks no prueban. No puede pasar directamente a producción. Antes de
iniciar, un humano debe aprobar el plan y verificar que la VM es nueva, descartable
y carece de montajes, sockets, credenciales y acceso administrativo a producción.
Usar imagen, configuración y autenticación ficticias propias del laboratorio;
no copiar claves, tokens, OAuth ni configuración sensible del entorno real.

Primera validación en desarrollo:

    PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -s tests -v

Guardar resultados, hash del candidato y referencia. La suite vigente tiene
122 pruebas, con Docker/procesos/señales simulados y filesystem/flock temporales.
La normalización de UID de / y /tmp del sandbox existe sólo en tests; no debe
trasladarse a política de producción.

## Matriz obligatoria del laboratorio futuro

1. Ubuntu/Python/kernel objetivos: pidfd_open, pidfd_send_signal, POLLIN real,
   zombie antes/durante lecturas /proc, reap, proceso ausente, PID reutilizado y
   permisos denegados. Nunca aceptar fallback a kill(pid).
2. Launcher: matar ejecutor antes de Popen, antes de persistir identidad, antes de
   permiso, después de permiso y durante exec. Comprobar EOF/timeout, que no se
   ejecuta gateway sin identidad durable y que exec conserva identidad de kernel.
3. Dos usuarios reales: operador y agente distintos, UID/GID no 10001, propietario
   y modos de workspace/runtime, acceso al socket, ACL, symlinks y ancestros
   modificables. Revisar privilegios del gateway; hereda identidad del operador.
4. Docker de laboratorio: create/start/TERM/wait/rm sin fuerza, estados paused,
   restarting, exited, límites/montajes y salida inmediata. Comparar comandos con
   la versión instalada y validar cierre de Node/Bridge, flush y consistencia.
5. Inyectar pérdida de respuesta después del efecto de create/start/rm, caída del
   daemon, timeouts y aparición tardía de un contenedor. Verificar conservación
   de journals inciertos y que un error posterior a start no mata la carga.
6. Concurrencia entre procesos independientes, timeout del lock, crash de su dueño
   y doble create/destroy. Los threads sobre un archivo temporal ya se probaron;
   no equivalen a validar namespaces, unidades o procesos del sistema real.
7. Reinicio sólo de la VM descartable autorizada: /run perdido, estado /var/lib
   conservado, contenedor detenido y recuperación por boot anterior. Inyectar
   fallos de fsync/almacenamiento y comprobar estados closed/temporales/residuos.
8. Contrato Bridge: formato SESSION_CREATED con nombre nuevo más largo, ausencia
   de gateway.pid y metadata legacy, ruta del socket montada, errores
   CREATE_INCOMPLETE_REVIEW_REQUIRED y CREATE_ROLLBACK_INCOMPLETE. No debe lanzar
   limpieza ciega o reintentar create como si no existieran recursos.

No marcar una casilla aprobada basándose sólo en mocks. Cada ensayo debe guardar
evidencia y limitaciones; cualquier fallo bloquea un despliegue futuro.

## Diseño de migración que excluye al ejecutor antiguo

1. Inventariar fuera de esta tarea las versiones y llamadores reales, configuración
   legacy, prefijo, workspaces y lectores de gateway.pid. No copiar credenciales.
   La migración no adopta registros v1/v2 ni contenedores existentes.
2. Bloquear nuevas invocaciones del ejecutor anterior mediante el mecanismo
   operativo aprobado. Revocar o retirar sus entrypoints/lanzadores y confirmar
   que no quedan procesos ejecutores pendientes ni solicitudes Docker inciertas.
   Si esto requiere cambiar servicios/permisos, obtener autorización específica.
3. Mantener inmutable config.json legacy: no darle prefijos nuevos ni apuntarlo a
   las raíces nuevas. Publicar en un futuro release config.v3.json separado. No
   cambiar symlinks/config compartida para alternar formatos.
4. Preaprovisionar STATE_ROOT y GATEWAY_ROOT v3, lock privado y workspace nuevo.
   Todas las raíces son privadas, con ancestros root/operador sin escritura de
   agentes; sin symlinks, aliases de raíz ni solapamientos. No reutilizar la raíz
   legacy. Evaluar bind mounts anidados y backups con el operador.
5. La configuración v3 requiere lifecycle_schema=3, deployment_id aleatorio de
   32 hex, legacy_executor_fenced=true, legacy_container_prefix correcto,
   legacy_workspace_root correcto, container_prefix y workspace_root nuevo;
   además agent_uid/gid enteros positivos, agente distinto de root/operador,
   pids_limit entero, memory con unidad k/m/g opcional, cpus positivo e image.
   Archivo 0600 propiedad del usuario efectivo. La atestación sólo se establece
   después de completar el fencing, no para evitar un error de validación.
6. Con el prefijo antiguo declarado se verifica que ningún SESSION_ID legacy
   válido de hasta 40 caracteres pueda nombrar un contenedor v3. Runtime, config y
   workspace separados impiden que el CLI anterior encuentre o reutilice esos
   recursos siguiendo su configuración original. Ninguna de estas barreras
   impide actuar a root con una configuración modificada o Docker directo.
7. Drenar sesiones antiguas mediante procedimientos específicos revisados antes
   de habilitar agentes en la nueva raíz. No ejecutar destructores antiguos sobre
   sesiones v3 ni el candidato sobre sesiones antiguas. Excluir siempre
   duality-sol-sandbox de cualquier procedimiento.
8. Transferir proyectos sólo mediante copia/snapshot supervisado con escritores
   detenidos y verificación de propietario/integridad. No mover ni borrar el origen
   hasta una aprobación independiente. No compartir un workspace activo entre
   generaciones. Esta entrega no contiene un migrador que copie datos.
9. Validar una sesión descartable, recover no destructivo, cierre confirmado,
   reintento y rollback en laboratorio. La ventana, responsables, mecanismos de
   fencing y comandos operativos deben aprobarse antes de un despliegue real.

## Rollback que no entrega v3 al destructor legacy

1. Bloquear nuevas creaciones v3 por el coordinador aprobado; no habilitar todavía
   el ejecutor anterior. Conservar el artefacto v3 y su configuración, journals,
   deployment ID, runtime y workspaces. No cambiar prefijos ni borrar registros.
2. Resolver sesiones starting/active/stopping o inciertas usando una versión v3
   revisada que conserve ID/etiquetas, pidfd, cierre cooperativo y lectura del
   esquema actual. Un rollback del binario no autoriza abandonar cargas vivas.
   No apuntar jamás el destructor legacy a nombres/directorios/workspaces v3.
3. Si una sesión no puede cerrarse con seguridad, mantenerla aislada y detener el
   rollback operativo hasta revisión humana. No forzar SIGKILL/rm -f para cumplir
   un plazo. Workspaces y evidencia permanecen disponibles.
4. Confirmar con inventario independiente que no hay ejecutores v3 pendientes,
   cargas nuevas ni operaciones Docker inciertas. Sólo después evaluar restaurar
   un release anterior en su namespace/config/workspace legacy intactos. Un artefacto
   anterior que no lea v3 no es herramienta de recuperación de v3.
5. Si se necesita recuperar datos hacia legacy, realizar copia supervisada con
   escritores detenidos y aprobación explícita. No reutilizar directamente raíces
   v3, no cambiar legacy_container_prefix para alcanzar nombres nuevos y no
   eliminar el origen como parte del rollback.
6. Restaurar versión/configuración/permisos por el proceso de releases autorizado.
   Cualquier reinicio o reconfiguración de servicio requiere autorización aparte.
   Restaurar el ejecutor vulnerable anterior restaura sus vulnerabilidades; la
   recomendación es conservar un controlador v3 corregido para cierre o un release
   compatible con el mismo esquema, no volver sin restricciones al destructor viejo.

## Aborto, observación y aprobaciones

Abortar ante identidad/etiquetas discordantes, raíces no confiables, ausencia de
pidfd, cierre que no confirma salida, corrupción, falsa creación exitosa, acceso
legacy a namespaces v3 o pérdida de datos. No «reparar» eliminando evidencia.
Observar cuarentenas, latencia del lock, solicitudes inciertas, errores de cierre
y discrepancias de estado. No instalar un recuperador destructivo al arranque.

Pendientes de aprobación humana: ejecución del laboratorio, política de privilegios
del gateway y servicio, tiempos de gracia, fencing de llamadores antiguos, copia
de proyectos, política de intervención en cuarentenas, fecha de despliegue y
procedimiento operativo de rollback. No se realizó ninguna de esas operaciones.
