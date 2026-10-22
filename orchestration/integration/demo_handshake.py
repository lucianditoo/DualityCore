"""DUALITY CORE - Operacion HANDSHAKE: Demostracion reproducible DMAOL <-> Executor."""
import hashlib
from pathlib import Path
import tempfile
import time
from typing import Dict, Any
import uuid

from orchestration.models import (
    TaskEnvelope,
    TaskPhase,
)
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.integration.adapter_ledger import AdapterLedger
from orchestration.integration.simulated_transport import SimulatedExecutorTransport
from orchestration.integration.executor_adapter import DmaolExecutorAdapter, canonical_uuid_for_task
from orchestration.integration.contract_messages import CONTRACT_VERSION, dump_strict_json


def run_handshake_demo(temp_dir: str, verbose: bool = True) -> Dict[str, Any]:
    """Execute reproducible handshake demonstration between DMAOL and Executor."""
    report = {
        "steps_completed": [],
        "observed_states": [],
        "task_id": None,
        "attempt_id": None,
        "session_id": None,
        "generation": None,
    }

    def log(msg: str):
        if verbose:
            print(f"[HANDSHAKE] {msg}")

    log("Iniciando Operacion HANDSHAKE (DMAOL <-> Executor v3 Contract)...")

    base_path = Path(temp_dir)
    dmaol_dir = base_path / "dmaol_store"
    ledger_dir = base_path / "adapter_ledger"

    store = TaskStore(str(dmaol_dir))
    lease_mgr = LeaseManager(store)
    ledger = AdapterLedger(str(ledger_dir))
    transport = SimulatedExecutorTransport(ledger)

    deployment_id = "0123456789abcdef0123456789abcdef"
    adapter = DmaolExecutorAdapter(transport, deployment_id=deployment_id)

    # 1. DMAOL crea tarea
    log("1. DMAOL crea TareaEnvelope...")
    task = TaskEnvelope.create(
        idempotency_key="task-handshake-e2e-001",
        title="Validar canal de control e integracion DMAOL-Executor",
        instructions="Verificar handshake formal segun dmaol-executor/1",
        target_branch="duality/task-handshake/executor-01",
        base_commit="base-sha-handshake-001",
        affected_paths=["src/driver.py"]
    )
    store.save_task(task)
    report["task_id"] = task.task_id
    report["steps_completed"].append("TASK_CREATED")
    log(f"   Tarea creada: {task.task_id} (phase={task.phase.value})")

    # 2. Adquiere lease (fencing_token = generation = 1)
    log("2. DMAOL adquiere lease bajo autoridad durable...")
    fencing_token = lease_mgr.acquire_lease(task, agent_id="executor-agent-01", ttl_seconds=300.0)
    report["generation"] = fencing_token
    report["steps_completed"].append("LEASE_ACQUIRED")
    log(f"   Lease concedido: agent={task.assigned_agent}, generation/fencing_token={fencing_token}")

    # 3. DMAOL solicita inicio (start) al Executor
    log("3. Enviando solicitud de inicio (op=start) al adaptador...")
    attempt_id = str(uuid.uuid4())
    report["attempt_id"] = attempt_id
    start_resp = adapter.start_attempt(
        task=task,
        authorization_ref="approval-handshake-phase3b",
        profile_ref="isolated-task-v1",
        input_ref="artifact-task-spec-001",
        attempt_id=attempt_id,
    )
    session_id = start_resp["session_id"]
    report["session_id"] = session_id
    report["observed_states"].append((start_resp["task_state"], start_resp["resource_state"], start_resp["certainty"]))
    report["steps_completed"].append("START_ACCEPTED")
    log(f"   Respuesta start recibida: session_id={session_id}, task_state={start_resp['task_state']}, resource_state={start_resp['resource_state']}")

    # 4. Executor provisiona recursos y despacha
    log("4. Simulando aprovisionamiento en Executor y transicion a RUNNING...")
    driver_obs = transport.simulate_executor_lifecycle_progress(
        task_id=canonical_uuid_for_task(task.task_id),
        expected_version=start_resp["state_version"],
        target_task_state="RUNNING",
    )
    report["observed_states"].append((driver_obs["task_state"], driver_obs["resource_state"], driver_obs["certainty"]))
    report["steps_completed"].append("RESOURCES_ACTIVE_OBSERVED")
    log(f"   Recursos activos observados: instance={driver_obs['executor_instance']}, task_state={driver_obs['task_state']}")

    task.transition_to(TaskPhase.RUNNING)
    store.save_task(task)
    log(f"   DMAOL sincroniza tarea a RUNNING (v={task.state_version})")

    # 5. Despacho y entrega de resultado autenticado por el Executor
    log("5. Simulando produccion de resultado autenticado por la sesion...")
    result_id = str(uuid.uuid4())
    artifact_hash = hashlib.sha256(b"handshake-artifact-output").hexdigest()
    raw_result = {
        "version": CONTRACT_VERSION,
        "task_id": canonical_uuid_for_task(task.task_id),
        "attempt_id": attempt_id,
        "generation": fencing_token,
        "session_id": session_id,
        "result_id": result_id,
        "outcome": "SUCCEEDED",
        "artifact_ref": "output-handshake-001.json",
        "artifact_sha256": artifact_hash,
        "completed_at": time.time(),
        "signature": f"sig-sim-{session_id[:12]}"
    }

    # Despacho al transporte
    res_bytes = transport.dispatch_execution_result(dump_strict_json(raw_result))
    report["steps_completed"].append("RESULT_DISPATCHED_AND_RECORDED")
    log("   Resultado validado y registrado en ledger durable")

    # 6. DMAOL convierte y acepta atomicamente el resultado
    log("6. DMAOL verifica version y fencing_token y acepta resultado...")
    dmaol_result = adapter.translate_to_dmaol_execution_result(raw_result)
    updated_task = lease_mgr.submit_execution_result(
        task_id=task.task_id,
        result=dmaol_result,
        expected_fencing_token=fencing_token,
        expected_version=task.state_version,
    )
    report["steps_completed"].append("DMAOL_RESULT_ACCEPTED")
    log(f"   DMAOL transiciono tarea a {updated_task.phase.value} con fencing_token={fencing_token}")

    # 7. Verificacion de persistencia tras reinicio en frio
    log("7. Verificando recuperacion tras reinicio en frio...")
    fresh_ledger = AdapterLedger(str(ledger_dir))
    fresh_store = TaskStore(str(dmaol_dir))

    reloaded_task = fresh_store.load_task(task.task_id)
    assert reloaded_task.phase == TaskPhase.AWAITING_REVIEW
    assert reloaded_task.execution_result.commit_sha is None
    assert "output-handshake-001.json" in reloaded_task.execution_result.generated_artifacts
    assert reloaded_task.execution_result.execution_time_seconds == 0.0

    obs_req = {
        "version": CONTRACT_VERSION,
        "op": "observe",
        "request_id": str(uuid.uuid4()),
        "authorization_ref": "auth-verify-restart",
        "deployment_id": deployment_id,
        "task_id": canonical_uuid_for_task(task.task_id),
        "attempt_id": attempt_id,
        "generation": fencing_token,
        "session_id": session_id,
    }
    reloaded_obs = fresh_ledger.observe(obs_req)
    assert reloaded_obs["task_state"] == "SUCCEEDED"
    assert reloaded_obs["session_id"] == session_id
    report["steps_completed"].append("PERSISTENCE_RECOVERY_VERIFIED")
    log("   Evidencia y estado recuperados intactos tras reinicio.")

    log("=== OPERACION HANDSHAKE COMPLETADA CON EXITO ===")
    return report


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        run_handshake_demo(temp_dir, verbose=True)
