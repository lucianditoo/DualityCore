# IGNITION — actualización FORGE LINK 3D-C2

Esta sección reemplaza las descripciones de arquitectura y supervisión de 3D-A
conservadas más abajo como registro histórico. El backend local ahora usa
`lab/handshake/ledger.py` como **única autoridad**. No existe la tabla generations
anterior de IGNITION; proceso y artefactos se vinculan a sus asignaciones contractuales.
`process_runner.py` no decide generaciones, no acepta resultados y no publica.

Se eliminó todo escalado automático a SIGKILL. Recover sólo registra conocimiento:
no crea cancelación y no causa señales indirectas. Un resultado inicialmente
UNKNOWN puede resolverse mediante evidencia posterior verificada del supervisor
que todavía posee al hijo. Revocar una generación tampoco envía señales.

Timeout se registra antes de confirmar salida. La API local Laboratory habilita
SIGTERM por timeout como política explícita y durable del perfil de demostración;
el adaptador contractual no lo habilita. La cancelación explícita autorizada permite
un único SIGTERM al hijo propio. Si lo ignora, la supervisión y el drenado continúan
sin SIGKILL hasta la salida natural; mientras tanto se conservan identidad y estado
incierto. No existe garantía de respuesta final acotada ni recuperación destructiva
tras perder el supervisor. `Backend.close()` sigue rechazando procesos propios activos.

Las 19 pruebas originales se adaptaron al almacenamiento compartido, workspaces
`<root>/<session_id>` y recuperación no destructiva. Se conservaron sus objetivos:
las inyecciones SQL ahora usan model/runs, el worker de crash actúa antes del commit
común y la prueba que esperaba terminación indirecta ahora exige salida natural
exitosa. Otras cuatro pruebas verifican los nuevos invariantes: **23 en total**.
El programa resistente es un fixture fijo de test, seleccionado sólo mediante
instrumentación Python de la prueba; ninguna solicitud puede seleccionarlo.

Los comandos de pruebas y demo de 3D-A siguen vigentes. Los ledgers experimentales
3D-A no se migran automáticamente: se rechazan. Crear un laboratorio nuevo es una
acción explícita; no se trata como recuperación de intentos anteriores. No se borró
ni migró ningún ledger existente. Ver [FORGE LINK](../forge_link/README.md) para la
arquitectura actual, publicaciones y limitaciones.

---

# IGNITION 3D-A — Executor local experimental

Baseline: `60ce2db62da97005db5cb267bc8a1e53eea55abf`, rama
`codex/executor-v3`. Este backend ejecuta **un proceso real**; no importa el Executor
existente ni el simulador handshake. No implementa ni modifica `dmaol-executor/1`.
Es una API Python local confiable, sin endpoint DMAOL ni transporte.

## Comandos reproducibles

Desde la raíz del repositorio, como usuario **no root**, Python 3.12:

```sh
python3 -B -m unittest discover -s tests/ignition -v
python3 -B -m lab.ignition.demo
```

La demostración no acepta argumentos: crea un laboratorio temporal, ejecuta el único
perfil, vuelve a calcular el hash del archivo y muestra ejecución, duración y salida
confirmada. Luego elimina exclusivamente su directorio temporal. No requiere paquetes,
privilegios, Docker, sockets, red, SSH, Bridge ni servicios. No cambia permisos ni
políticas del host. Los workers auxiliares de pruebas también son procesos locales
reales y sólo se ejecutan con datos ficticios en el laboratorio temporal.

## Programa permitido y frontera

`isolated-echo-v1` selecciona exclusivamente `echo_program.py`, una ruta fija del
repositorio. Se invoca con el intérprete Python actual resuelto a ruta absoluta,
`-I -B`, sin shell, sin argumentos para el programa, stdin cerrado, descriptores
heredados cerrados y entorno reemplazado por LANG/LC_ALL fijos. No se heredan secretos
del entorno, PYTHONPATH ni configuración de Python del usuario. El programa también
rechaza root y argumentos. La instalación Python y los archivos del repositorio son
parte de la base confiable; no hay verificación criptográfica del código instalado.

El payload siempre escribe 128 KiB de `O` en stdout, 128 KiB de `E` en stderr,
espera 0.4 segundos y produce `artifact.txt` con exactamente:

```text
IGNITION isolated-echo-v1
```

Incluye un salto de línea final. SHA-256 esperado:
`acad81346d1f2213d8236cedd2a4f8220361f6f189d120d667ba8e9eab5da8ea`.
No lee tareas, código, rutas, argumentos, variables ni input de DMAOL. No ejecuta
procesos descendientes ni importa herramientas del Executor. No se debe sustituir
el programa fijo para ejecutar cargas externas.

`Laboratory()` crea una raíz `ignition-*` directamente bajo el directorio temporal
del sistema. Cada ejecución crea su propio subdirectorio aleatorio `run-*`; los IDs
nunca se convierten en rutas. `Backend(root)` sólo reabre esa clase de raíz temporal,
resuelta, de propietario actual y sin symlink en su ruta. Esto evita errores de uso,
**no es una frontera frente a un atacante del mismo usuario**. No se admiten rutas
de producción. No hay campos que permitan seleccionar otro ejecutable o workspace.

## API local

```python
from lab.ignition.backend import Laboratory
# Dentro de un contexto Laboratory, usando UUID canónicos no nulos ficticios:
# backend.advance(task_id, attempt_id, generation)
# backend.start(request_id=..., task_id=..., attempt_id=..., generation=...,
#               profile='isolated-echo-v1', timeout_ms=2000)
# backend.execute(request_id)   # Bloquea mientras supervisa a su hijo directo.
# backend.observe(request_id)
# backend.cancel(request_id)    # Registra intención; el supervisor cierra al hijo.
# backend.recover(request_id)   # Marca incompletos UNKNOWN; jamás relanza/adopta.
```

`advance` representa una autoridad **de laboratorio**, independiente de la generation
enviada a start. No es autorización humana ni autoridad DMAOL distribuida. Sólo
permite aumentos por tarea y attempt_id nunca reutilizados. Start valida generation
contra esa autoridad, UUID y timeout entero (no bool) de 1 a 5000 ms; sólo admite el
perfil fijo. Campos `command`, `args`, `cwd`, `env`, `input_ref` no existen en su API.
Esta interfaz no está habilitada para clientes externos ni tareas DMAOL.

Start registra INTENT con un execution_id UUID antes de cualquier proceso.
Execute reclama SPAWNING de forma transaccional antes de Popen y luego registra
RUNNING. Otro execute simultáneo ve el estado existente y no lanza otro proceso;
puede devolver SPAWNING/RUNNING, por lo que el llamador consulta observe. El mismo
request_id con contenido distinto, u otra clave para el mismo intento, se rechaza
como IDEMPOTENCY_CONFLICT. Un reintento idéntico conserva execution_id y devuelve
el estado actual; no es el protocolo de ACK almacenado del simulador handshake.

Los estados terminales son SUCCEEDED, FAILED, TIMED_OUT, CANCELLED y UNKNOWN.
Cancel antes de ejecutar deja CANCELLED sin crear proceso. Durante ejecución sólo
registra CANCELLING; el supervisor observa esa intención, termina su hijo y espera
su salida antes de confirmar CANCELLED. Un resultado ya confirmado se conserva.
Si no se demuestra el orden entre finalización y cancelación, se publica UNKNOWN.
Una cancelación de generation antigua se rechaza, incluso si el intento ya terminó.

## Supervisión, resultado y persistencia

Se drenan ambos pipes con selectors para evitar bloqueo por buffers llenos. Se
conservan como máximo 4096 bytes por stream y se cuentan los bytes recibidos; el
resto se descarta. El resultado informa truncamiento. El payload fijo sólo emite
ASCII. Se mide duración con reloj monotónico desde antes de Popen hasta después de
salida/verificación del artefacto, antes del commit final. No es tiempo de CPU ni
un plazo durable reconstruido tras reinicio.

Al vencer timeout o recibir cancelación se envía terminate al **hijo directo aún
poseído por Popen**; tras 250 ms se permite kill del mismo hijo si sigue vivo. Se
espera y recolecta la salida del hijo; nunca se señaliza un PID leído del ledger.
No se matan grupos, procesos adoptados ni recursos ajenos. El programa fijo no crea
descendientes. La ruta de escalado existe como contingencia; el programa normal
responde a terminate, por lo que las pruebas no acreditan kill frente a un proceso
que ignore señales. Un deadline total no está garantizado: arranque, SQLite, fsync,
I/O de kernel o espera de un proceso ininterrumpible pueden bloquear.

Para SUCCEEDED se exige retorno cero y contenido exacto del artefacto. Se abre con
O_NOFOLLOW, exige archivo regular, lee una cantidad acotada, calcula SHA-256 de los
bytes observados y hace fsync del archivo y su directorio antes del commit del
resultado. El resultado contiene execution_id, attempt_id, generation, outcome,
accepted_as_current, hash, nombres relativos de workspace/artefacto, duración,
returncode, exit_confirmed, stdout/stderr limitados y contadores/truncamiento.
No se exporta ni publica el artefacto fuera del laboratorio.

SQLite usa synchronous=FULL y BEGIN IMMEDIATE para serializar autoridad histórica,
intentos, cancelaciones y aceptación del resultado. La comprobación final de
generation y la escritura del resultado comparten transacción. Si se revoca el
intento antes de aceptar su resultado, éste queda UNKNOWN y no vigente; no afecta
el registro del intento nuevo. La generación puede avanzar mientras el proceso viejo
termina, porque este payload no tiene efectos externos y cada intento usa cwd propio;
esto **no autoriza esa política para cargas generales**. accepted_as_current describe
la decisión histórica en el commit, no una autorización que sobreviva a generaciones
posteriores. Reproducir ese registro antiguo no vuelve a aceptar ni publicar nada.

Al reabrir no se lanza ningún proceso. Recover convierte INTENT/SPAWNING/RUNNING/
CANCELLING en UNKNOWN y preserva la identidad; el paso del tiempo, un artefacto
presente o un PID no convierten incertidumbre en éxito. Incluso INTENT se trata
conservadoramente en esta operación. Un ledger faltante se abre con mode=rw y falla,
sin recrearlo; identidad ausente, fingerprints y resultados inconsistentes también
bloquean operaciones. No hay reparación, purga, relanzamiento ni adopción automática.
Si falla persistir el resultado después de la salida, se conserva la incertidumbre.

Las excepciones de launch/I/O se traducen en UNKNOWN cuando el ledger puede escribirse;
fallos del propio ledger se propagan, sin ACK de éxito. Los datos y artefactos se
conservan durante la vida del laboratorio. `Backend.close()` no elimina archivos y
rechaza cerrar mientras ese objeto supervisa una ejecución. Quien utilice varias
instancias/procesos debe esperar a **todos** antes de cerrar `Laboratory`: su cierre
es una eliminación explícita de datos temporales, no un recolector de orfandad.

## Evidencia ejecutada y límites

La suite tiene **19 pruebas**. Ejecuta el programa real para éxito/hash/duración,
saturación de ambos pipes, timeout, cancelación durante ejecución desde otra conexión,
fallo real del artefacto, idempotencia tras reabrir y dos reclamaciones concurrentes.
Comprueba cancelación previa sin spawn, generaciones obsoletas antes/durante trabajo,
resultado viejo sin cambios sobre el nuevo, recuperación conservadora durante un
proceso vivo y fases incompletas inyectadas, corrupción y rechazo de root/input libre.

Dos pruebas usan intérpretes auxiliares independientes que ejecutan y **recolectan**
el hijo real antes de caer deliberadamente: una sale con UPDATE del resultado aún
sin COMMIT; la otra sale después de COMMIT y antes del ACK. Al reabrir, la primera
queda UNKNOWN pese al artefacto correcto y no relanza; la segunda conserva SUCCEEDED
sin otro proceso. Las pruebas no dejan hijos huérfanos intencionadamente. No acreditan
muerte del supervisor mientras el hijo sigue vivo: en ese caso un proceso local
puede continuar, y este backend no ofrece recuperación destructiva de huérfanos.

Un subprocess local **no equivale a un contenedor o sandbox**. Tiene los permisos del
usuario y podría acceder a sus archivos o red si se cambiara el programa. No se crean
namespaces, cgroups, restricciones de red, límites de CPU/RAM, filtros de syscalls ni
políticas de archivos. La ausencia de red se debe al código fijo y a no proporcionar
transporte, no a una barrera de aislamiento del sistema operativo. No se prueban
cargas arbitrarias, autenticación externa, publicación atómica en receptores externos,
efectos exactly-once, hardware duradero, corte de energía, sustitución maliciosa del
ledger/artefacto ni snapshots atrasados. El hash acredita los bytes leídos, no
inmutabilidad posterior ni procedencia criptográfica del ejecutor.

Este slice no modifica Executor v3, IRONCLAD, handshake, servicios o producción.
No demuestra integración DMAOL ni autoriza cargas externas o despliegue. No se
realizan commit ni push en esta fase.
