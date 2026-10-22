"""DUALITY CORE - Operacion FIRST CONTACT: Demostracion reproducible E2E.

Ejecuta el ciclo de vida completo de orquestacion multiagente utilizando
exclusivamente componentes simulados (codex-sim y antigravity-sim).
"""
import os
from pathlib import Path
import tempfile
import time
from typing import Dict, Any

from orchestration.errors import (
    ResourceConflictError,
    FencingTokenMismatchError,
    SecurityViolationError,
    IdempotencyConflictError,
    IllegalStateTransitionError,
)
from orchestration.models import (
    TaskEnvelope,
    TaskPhase,
    ExecutionResult,
    GitMergeRecord,
    HumanApproval,
)
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.contracts import (
    SimulatedSessionDriver,
    CryptographicHumanGate,
)
from orchestration.git_policy import GitPolicy


def run_first_contact_demo(temp_dir: str, verbose: bool = True) -> Dict[str, Any]:
    """Ejecuta la suite de demostracion completa E2E en un directorio temporal aislado."""
    report = {
        "tasks_completed": [],
        "rejections_tested": [],
        "negative_tests_passed": [],
        "transitions_observed": [],
    }

    def log(msg: str):
        if verbose:
            print(f"[FIRST_CONTACT] {msg}")

    log("Iniciando Operacion FIRST CONTACT...")
    store = TaskStore(temp_dir)
    lease_mgr = LeaseManager(store)
    session_driver = SimulatedSessionDriver()
    human_gate = CryptographicHumanGate()
    test_operator_secret = b"ephemeral-demo-secret-key-32bytes"

    # =========================================================================
    # 1. FLUJO COMPLETO APROBADO: CODEX-SIM (Ruta Backend)
    # =========================================================================
    log("\n--- 1. Ejecutando Flujo Aprobado: codex-sim ---")
    task_codex = TaskEnvelope.create(
        idempotency_key="codex-task-e2e-001",
        title="Implementar driver de persistencia transaccional",
        instructions="Agregar soporte para locking de kernel interproceso",
        target_branch="duality/task-backend-001/codex-sim",
        base_commit="base-sha-codex-100",
        affected_paths=["src/backend/persistence_driver.py"]
    )
    GitPolicy.validate_agent_branch(task_codex.target_branch)
    store.save_task(task_codex)
    report["transitions_observed"].append((task_codex.task_id, None, TaskPhase.PENDING.value))
    log(f"Tarea Codex creada: {task_codex.task_id} (PENDING)")

    # Asignacion de lease a codex-sim
    session_driver.create_session("sess-codex-01")
    token_codex = lease_mgr.acquire_lease(task_codex, agent_id="codex-sim", ttl_seconds=300.0)
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.PENDING.value, TaskPhase.ASSIGNED.value))
    log(f"Lease adquirido por codex-sim (gen={token_codex}, ASSIGNED)")

    # Inicio de ejecucion
    task_codex.transition_to(TaskPhase.RUNNING)
    store.save_task(task_codex)
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.ASSIGNED.value, TaskPhase.RUNNING.value))
    log(f"Codex inicio trabajo (RUNNING, v={task_codex.state_version})")

    # Entrega de resultado simulado
    res_codex = ExecutionResult(
        status="SUCCESS",
        summary="Implementado CrossPlatformFileLock con soporte msvcrt/fcntl",
        commit_sha="a1b2c3d4e5f60011223344556677889900aabbcc",
        generated_artifacts=["persistence_report.json"],
        execution_time_seconds=22.4,
        completed_at=time.time(),
        fencing_token_used=token_codex
    )

    # Aceptacion atomica
    task_codex = lease_mgr.submit_execution_result(
        task_codex.task_id,
        result=res_codex,
        expected_fencing_token=token_codex,
        expected_version=task_codex.state_version
    )
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.RUNNING.value, TaskPhase.AWAITING_REVIEW.value))
    log(f"Resultado aceptado atomicamente (AWAITING_REVIEW, fencing_token={token_codex})")

    # Aprobacion humana autenticada
    approval_codex = human_gate.submit_decision(
        task_codex,
        approver_id="lead-operator-human",
        decision="APPROVED",
        comments="Codigo verificado y pruebas multiproceso aprobadas",
        secret_key=test_operator_secret
    )
    human_gate.verify_decision(task_codex, approval_codex, test_operator_secret)
    task_codex.human_approval = approval_codex
    task_codex.transition_to(TaskPhase.APPROVED)
    store.save_task(task_codex)
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.AWAITING_REVIEW.value, TaskPhase.APPROVED.value))
    log("Aprobacion humana autenticada verificada (APPROVED)")

    # Merge seguro
    task_codex.transition_to(TaskPhase.MERGING)
    target_branch_codex = GitPolicy.validate_target_branch("main", allow_protected=True)
    task_codex.git_merge = GitMergeRecord(
        source_branch=task_codex.target_branch,
        target_branch=target_branch_codex,
        merged_commit_sha="cc99887766554433221100ffeeddccbbaa001122",
        merge_strategy="squash",
        merged_by=approval_codex.approver_id,
        merged_at=time.time()
    )
    task_codex.transition_to(TaskPhase.COMPLETED)
    store.save_task(task_codex)
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.APPROVED.value, TaskPhase.MERGING.value))
    report["transitions_observed"].append((task_codex.task_id, TaskPhase.MERGING.value, TaskPhase.COMPLETED.value))
    session_driver.destroy_session("sess-codex-01")
    report["tasks_completed"].append(("codex-sim", task_codex.task_id))
    log(f"Tarea Codex completada exitosamente: {task_codex.task_id} (COMPLETED)")

    # =========================================================================
    # 2. FLUJO COMPLETO APROBADO: ANTIGRAVITY-SIM (Ruta Frontend/Testing)
    # =========================================================================
    log("\n--- 2. Ejecutando Flujo Aprobado: antigravity-sim ---")
    task_ag = TaskEnvelope.create(
        idempotency_key="ag-task-e2e-002",
        title="Crear panel de monitoreo de agentes en tiempo real",
        instructions="Implementar componente React y suscripciones a SSE",
        target_branch="agent/antigravity-sim/task-ui-002",
        base_commit="base-sha-ag-200",
        affected_paths=["src/frontend/AgentMonitor.tsx", "src/frontend/useStream.ts"]
    )
    GitPolicy.validate_agent_branch(task_ag.target_branch)
    store.save_task(task_ag)
    report["transitions_observed"].append((task_ag.task_id, None, TaskPhase.PENDING.value))
    log(f"Tarea Antigravity creada: {task_ag.task_id} (PENDING)")

    # Asignacion de lease a antigravity-sim
    session_driver.create_session("sess-ag-01")
    token_ag = lease_mgr.acquire_lease(task_ag, agent_id="antigravity-sim", ttl_seconds=300.0)
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.PENDING.value, TaskPhase.ASSIGNED.value))
    log(f"Lease adquirido por antigravity-sim (gen={token_ag}, ASSIGNED)")

    # Inicio de ejecucion
    task_ag.transition_to(TaskPhase.RUNNING)
    store.save_task(task_ag)
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.ASSIGNED.value, TaskPhase.RUNNING.value))
    log(f"Antigravity inicio trabajo (RUNNING, v={task_ag.state_version})")

    # Entrega de resultado simulado
    res_ag = ExecutionResult(
        status="SUCCESS",
        summary="Componente AgentMonitor.tsx implementado y validado con tests unitarios",
        commit_sha="f0e1d2c3b4a50099887766554433221100ffeedd",
        generated_artifacts=["ui_coverage.html"],
        execution_time_seconds=18.1,
        completed_at=time.time(),
        fencing_token_used=token_ag
    )

    task_ag = lease_mgr.submit_execution_result(
        task_ag.task_id,
        result=res_ag,
        expected_fencing_token=token_ag,
        expected_version=task_ag.state_version
    )
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.RUNNING.value, TaskPhase.AWAITING_REVIEW.value))
    log(f"Resultado aceptado atomicamente (AWAITING_REVIEW, fencing_token={token_ag})")

    approval_ag = human_gate.submit_decision(
        task_ag,
        approver_id="lead-operator-human",
        decision="APPROVED",
        comments="UI y accesibilidad aprobadas",
        secret_key=test_operator_secret
    )
    human_gate.verify_decision(task_ag, approval_ag, test_operator_secret)
    task_ag.human_approval = approval_ag
    task_ag.transition_to(TaskPhase.APPROVED)
    store.save_task(task_ag)
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.AWAITING_REVIEW.value, TaskPhase.APPROVED.value))

    task_ag.transition_to(TaskPhase.MERGING)
    target_branch_ag = GitPolicy.validate_target_branch("main", allow_protected=True)
    task_ag.git_merge = GitMergeRecord(
        source_branch=task_ag.target_branch,
        target_branch=target_branch_ag,
        merged_commit_sha="11223344556677889900aabbccddeeff00112233",
        merge_strategy="merge-commit",
        merged_by=approval_ag.approver_id,
        merged_at=time.time()
    )
    task_ag.transition_to(TaskPhase.COMPLETED)
    store.save_task(task_ag)
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.APPROVED.value, TaskPhase.MERGING.value))
    report["transitions_observed"].append((task_ag.task_id, TaskPhase.MERGING.value, TaskPhase.COMPLETED.value))
    session_driver.destroy_session("sess-ag-01")
    report["tasks_completed"].append(("antigravity-sim", task_ag.task_id))
    log(f"Tarea Antigravity completada exitosamente: {task_ag.task_id} (COMPLETED)")

    # =========================================================================
    # 3. REENVIO IDEMPOTENTE
    # =========================================================================
    log("\n--- 3. Probando Reenvio Idempotente ---")
    duplicate_task = store.find_by_idempotency_key("codex-task-e2e-001")
    assert duplicate_task is not None
    assert duplicate_task.task_id == task_codex.task_id
    assert duplicate_task.phase == TaskPhase.COMPLETED
    log("Reenvio de idempotency_key devuelve la tarea existente sin duplicacion")

    # Reenvio con carga en conflicto arroja IdempotencyConflictError
    conflicting = TaskEnvelope.create(
        idempotency_key="codex-task-e2e-001",
        title="Otro titulo diferente",
        instructions="Otras instrucciones",
        target_branch="duality/task-backend-001/codex-sim",
        base_commit="base-sha-codex-100",
        affected_paths=["src/backend/persistence_driver.py"]
    )
    try:
        store.save_task(conflicting)
        raise AssertionError("Debio fallar con IdempotencyConflictError")
    except IdempotencyConflictError:
        log("Deteccion de payload en conflicto para idempotency_key verificada")
        report["negative_tests_passed"].append("IdempotencyConflictError")

    # =========================================================================
    # 4. RECHAZO DE FENCING TOKEN OBSOLETO
    # =========================================================================
    log("\n--- 4. Probando Rechazo de Fencing Token Obsoleto ---")
    task_fence = TaskEnvelope.create(
        idempotency_key="fence-test-003",
        title="Test de fencing",
        instructions="Validar token",
        target_branch="duality/task-fence/codex-sim",
        base_commit="c00",
        affected_paths=["src/core.py"]
    )
    store.save_task(task_fence)
    tok1 = lease_mgr.acquire_lease(task_fence, "codex-sim", ttl_seconds=1.0, now=10.0)

    # Expiracion y reasignacion tras confirmar muerte de ejecutor
    reconciled = lease_mgr.reconcile_expired_lease(task_fence.task_id, is_executor_confirmed_dead_fn=lambda aid: True, now=20.0)
    tok2 = lease_mgr.acquire_lease(reconciled, "antigravity-sim", ttl_seconds=100.0, now=25.0)
    reconciled.transition_to(TaskPhase.RUNNING)
    store.save_task(reconciled)

    stale_res = ExecutionResult("SUCCESS", "Late", "x" * 40, [], 10.0, 26.0, fencing_token_used=tok1)
    try:
        lease_mgr.submit_execution_result(
            reconciled.task_id,
            result=stale_res,
            expected_fencing_token=tok1,
            expected_version=reconciled.state_version,
            now=26.0
        )
        raise AssertionError("Debio rechazar token obsoleto")
    except FencingTokenMismatchError:
        log(f"Resultado tardio con gen={tok1} rechazado (esperado gen={tok2})")
        report["negative_tests_passed"].append("FencingTokenMismatchError")

    # =========================================================================
    # 5. CONFLICTO DE RECURSOS ENTRE AGENTES
    # =========================================================================
    log("\n--- 5. Probando Conflicto de Recursos (Path Leases) ---")
    t_res1 = TaskEnvelope.create("res-c1", "T1", "I", "duality/t1/codex-sim", "c", ["src/shared/config.json"])
    store.save_task(t_res1)
    lease_mgr.acquire_lease(t_res1, "codex-sim", ttl_seconds=300.0)

    t_res2 = TaskEnvelope.create("res-c2", "T2", "I", "agent/antigravity-sim/t2", "c", ["src/shared/config.json", "src/extra.py"])
    store.save_task(t_res2)

    try:
        lease_mgr.acquire_lease(t_res2, "antigravity-sim")
        raise AssertionError("Debio fallar con ResourceConflictError")
    except ResourceConflictError as e:
        log(f"Conflicto de ruta detectado: {e.details['resource']} retenido por {e.details['current_holder']}")
        report["negative_tests_passed"].append("ResourceConflictError")

    # =========================================================================
    # 6. APROBACION INVALIDA O FALSIFICADA
    # =========================================================================
    log("\n--- 6. Probando Aprobacion Invalida y Falsificada ---")
    task_auth_test = TaskEnvelope.create("auth-test-01", "T", "I", "duality/ta/codex-sim", "c", ["p.py"])
    store.save_task(task_auth_test)
    token_at = lease_mgr.acquire_lease(task_auth_test, "codex-sim")
    task_auth_test.transition_to(TaskPhase.RUNNING)
    store.save_task(task_auth_test)
    res_at = ExecutionResult("SUCCESS", "Done", "d" * 40, [], 5.0, time.time(), fencing_token_used=token_at)
    task_auth_test = lease_mgr.submit_execution_result(task_auth_test.task_id, res_at, token_at, task_auth_test.state_version)

    # Intento 1: Agente intentando auto-aprobarse
    try:
        human_gate.submit_decision(task_auth_test, "agent-codex-sim", "APPROVED", "Self approve", test_operator_secret)
        raise AssertionError("Debio rechazar auto-aprobacion de agente")
    except SecurityViolationError as e:
        log("Auto-aprobacion de agente bloqueada (AGENT_CANNOT_SELF_APPROVE)")
        report["negative_tests_passed"].append("AGENT_CANNOT_SELF_APPROVE")

    # Intento 2: Firma con clave secreta incorrecta (falsificacion)
    forged = human_gate.submit_decision(task_auth_test, "lead-operator-human", "APPROVED", "Forged", b"attacker-fake-secret-key-32b!")
    try:
        human_gate.verify_decision(task_auth_test, forged, test_operator_secret)
        raise AssertionError("Debio fallar con FORGED_HUMAN_APPROVAL")
    except SecurityViolationError as e:
        log("Firma HMAC invalida rechazada (FORGED_HUMAN_APPROVAL)")
        report["negative_tests_passed"].append("FORGED_HUMAN_APPROVAL")

    # =========================================================================
    # 7. FLUJO RECHAZADO (MODELO Y LIMITACIONES)
    # =========================================================================
    log("\n--- 7. Probando Flujos Rechazados Segun el Modelo ---")
    # Caso 7A: Rechazo humano para retrabajo (AWAITING_REVIEW -> PENDING)
    log("Probando Rechazo para Retrabajo (AWAITING_REVIEW -> PENDING)...")
    rejection_rework = human_gate.submit_decision(
        task_auth_test,
        approver_id="lead-operator-human",
        decision="REJECTED",
        comments="Cobertura insuficiente; reformular tests",
        secret_key=test_operator_secret
    )
    human_gate.verify_decision(task_auth_test, rejection_rework, test_operator_secret)
    task_auth_test.human_approval = rejection_rework
    task_auth_test.transition_to(TaskPhase.PENDING, reason="Rechazado para retrabajo")
    store.save_task(task_auth_test)
    report["transitions_observed"].append((task_auth_test.task_id, TaskPhase.AWAITING_REVIEW.value, TaskPhase.PENDING.value))
    report["rejections_tested"].append(("rework", task_auth_test.task_id, TaskPhase.PENDING.value))
    log(f"Tarea transiciono a PENDING para re-asignacion (v={task_auth_test.state_version})")

    # Caso 7B: Rechazo definitivo / Aborto administrativo (AWAITING_REVIEW -> CANCELLING -> CANCELLED)
    log("Probando Rechazo Administrativo / Cancelacion (AWAITING_REVIEW -> CANCELLING -> CANCELLED)...")
    task_abort = TaskEnvelope.create("abort-test-02", "T", "I", "duality/tabort/codex-sim", "c", ["p2.py"])
    store.save_task(task_abort)
    tok_ab = lease_mgr.acquire_lease(task_abort, "codex-sim")
    task_abort.transition_to(TaskPhase.RUNNING)
    store.save_task(task_abort)
    res_ab = ExecutionResult("SUCCESS", "Done", "e" * 40, [], 5.0, time.time(), fencing_token_used=tok_ab)
    task_abort = lease_mgr.submit_execution_result(task_abort.task_id, res_ab, tok_ab, task_abort.state_version)

    # El operador descarta la tarea definitivamente
    task_abort.transition_to(TaskPhase.CANCELLING, reason="Rechazo definitivo del operador")
    task_abort.transition_to(TaskPhase.CANCELLED, reason="Recursos limpios y tarea abortada")
    store.save_task(task_abort)
    report["transitions_observed"].append((task_abort.task_id, TaskPhase.AWAITING_REVIEW.value, TaskPhase.CANCELLING.value))
    report["transitions_observed"].append((task_abort.task_id, TaskPhase.CANCELLING.value, TaskPhase.CANCELLED.value))
    report["rejections_tested"].append(("cancelled", task_abort.task_id, TaskPhase.CANCELLED.value))
    log(f"Tarea abortada y cancelada: {task_abort.task_id} (CANCELLED)")

    # Caso 7C: Limitacion Documentada: AWAITING_REVIEW -> FAILED es ilegal en el modelo
    try:
        task_auth_test_2 = TaskEnvelope.create("test-illegal-fail", "T", "I", "duality/tif/codex", "c", ["p3.py"])
        store.save_task(task_auth_test_2)
        tok_if = lease_mgr.acquire_lease(task_auth_test_2, "codex")
        task_auth_test_2.transition_to(TaskPhase.RUNNING)
        store.save_task(task_auth_test_2)
        res_if = ExecutionResult("SUCCESS", "D", "f" * 40, [], 1.0, time.time(), tok_if)
        task_auth_test_2 = lease_mgr.submit_execution_result(task_auth_test_2.task_id, res_if, tok_if, task_auth_test_2.state_version)
        task_auth_test_2.transition_to(TaskPhase.FAILED)
        raise AssertionError("Debio ser rechazado como transicion ilegal")
    except IllegalStateTransitionError:
        log("Transicion AWAITING_REVIEW -> FAILED rechazada como ilegal conforme al modelo (Limitacion Arquitectonica Verificada)")
        report["negative_tests_passed"].append("AWAITING_REVIEW_TO_FAILED_ILLEGAL")

    # =========================================================================
    # 8. PERSISTENCIA Y RECUPERACION TRAS REINICIO
    # =========================================================================
    log("\n--- 8. Verificando Persistencia y Recuperacion tras Reinicio ---")
    # Instanciamos un nuevo TaskStore sobre el mismo directorio para simular reinicio del orquestador
    fresh_store = TaskStore(temp_dir)
    recovered, quarantined = fresh_store.recover_on_startup()

    assert task_codex.task_id in recovered
    assert task_ag.task_id in recovered

    reloaded_codex = fresh_store.load_task(task_codex.task_id)
    assert reloaded_codex.phase == TaskPhase.COMPLETED
    assert reloaded_codex.execution_result is not None
    assert reloaded_codex.execution_result.commit_sha == res_codex.commit_sha
    assert reloaded_codex.human_approval is not None
    assert reloaded_codex.human_approval.approver_id == "lead-operator-human"
    assert reloaded_codex.git_merge is not None
    assert reloaded_codex.git_merge.target_branch == "main"

    reloaded_ag = fresh_store.load_task(task_ag.task_id)
    assert reloaded_ag.phase == TaskPhase.COMPLETED
    assert reloaded_ag.execution_result.commit_sha == res_ag.commit_sha
    assert reloaded_ag.git_merge.target_branch == "main"

    log("Recuperacion tras reinicio confirmada: estado integro y auditable")
    report["negative_tests_passed"].append("PersistenceRecoveryVerified")

    log("\n=== OPERACION FIRST CONTACT COMPLETADA SATISFACTORIAMENTE ===")
    return report


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        run_first_contact_demo(temp_dir, verbose=True)
