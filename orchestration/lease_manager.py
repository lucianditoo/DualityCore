"""Lease management, fencing token generation, and concurrent resource access control."""
import time
from typing import Dict, Set, Optional, Callable, List, Tuple
import uuid

from orchestration.errors import (
    LeaseExpiredError,
    FencingTokenMismatchError,
    ResourceConflictError,
    QuarantineRequiredError,
    PermanentError,
)
from orchestration.models import (
    TaskEnvelope,
    TaskPhase,
    TaskLease,
    ExecutionResult,
)
from orchestration.persistence import TaskStore


class LeaseManager:
    """Manages exclusive task and file-path leases with atomic inter-process transactions."""

    def __init__(self, task_store: TaskStore):
        self.store = task_store

    def _get_active_path_locks_unlocked(self, exclude_task_id: str, current_time: float) -> Dict[str, Tuple[str, str]]:
        """Scan all stored tasks to determine currently active, non-expired path leases across all processes."""
        active_locks: Dict[str, Tuple[str, str]] = {}
        all_tasks = self.store._list_all_tasks_unlocked()

        for t in all_tasks:
            if t.task_id == exclude_task_id:
                continue
            if t.phase in (TaskPhase.ASSIGNED, TaskPhase.RUNNING) and t.lease:
                if not t.lease.is_expired(current_time):
                    for path in t.affected_paths:
                        norm = path.lower().replace('\\', '/').strip('/')
                        active_locks[norm] = (t.task_id, t.assigned_agent or "unknown")

        return active_locks

    def acquire_lease(
        self,
        task: TaskEnvelope,
        agent_id: str,
        ttl_seconds: float = 300.0,
        now: Optional[float] = None
    ) -> int:
        """Acquire an exclusive lease for a task and its affected paths under inter-process lock."""
        current_time = time.time() if now is None else now

        with self.store.lock():
            # Refresh task state from store to ensure latest version is checked
            fresh_task = self.store._load_task_unlocked(task.task_id)

            if fresh_task.phase not in (TaskPhase.PENDING, TaskPhase.ASSIGNED):
                raise PermanentError(
                    "TASK_NOT_LEASABLE",
                    f"Cannot acquire lease on task {fresh_task.task_id} in phase {fresh_task.phase.value}"
                )

            # Check persistent cross-process path conflicts
            active_locks = self._get_active_path_locks_unlocked(fresh_task.task_id, current_time)
            for path in fresh_task.affected_paths:
                norm_path = path.lower().replace('\\', '/').strip('/')
                if norm_path in active_locks:
                    holding_task_id, holding_agent = active_locks[norm_path]
                    raise ResourceConflictError(path, holding_agent)

            # Increment monotonic fencing generation
            new_generation = fresh_task.fencing_token + 1
            lease_id = f"lease-{uuid.uuid4().hex[:12]}"

            lease = TaskLease(
                lease_id=lease_id,
                holder_agent_id=agent_id,
                generation=new_generation,
                acquired_at=current_time,
                expires_at=current_time + ttl_seconds,
                ttl_seconds=ttl_seconds
            )

            fresh_task.lease = lease
            fresh_task.assigned_agent = agent_id
            fresh_task.fencing_token = new_generation
            fresh_task.transition_to(TaskPhase.ASSIGNED, reason=f"Lease acquired by {agent_id} (gen={new_generation})")

            # Persist atomically under the lock
            self.store._save_task_unlocked(fresh_task)

            # Sync in-memory representation
            task.lease = fresh_task.lease
            task.assigned_agent = fresh_task.assigned_agent
            task.fencing_token = fresh_task.fencing_token
            task.phase = fresh_task.phase
            task.state_version = fresh_task.state_version

            return new_generation

    def renew_lease(
        self,
        task_id: str,
        fencing_token: int,
        additional_ttl_seconds: Optional[float] = None,
        now: Optional[float] = None
    ) -> float:
        """Renew an existing lease under inter-process lock."""
        current_time = time.time() if now is None else now

        with self.store.lock():
            task = self.store._load_task_unlocked(task_id)

            if not task.lease:
                raise LeaseExpiredError(task_id, "NONE")

            task.verify_fencing_token(fencing_token)

            if task.lease.is_expired(current_time):
                raise LeaseExpiredError(task_id, task.lease.lease_id)

            ttl = additional_ttl_seconds if additional_ttl_seconds is not None else task.lease.ttl_seconds
            task.lease.expires_at = current_time + ttl
            task.updated_at = current_time
            self.store._save_task_unlocked(task)
            return task.lease.expires_at

    def release_lease(
        self,
        task_id: str,
        fencing_token: int,
        is_terminal: bool = False
    ) -> None:
        """Release active lease locks under inter-process lock."""
        with self.store.lock():
            task = self.store._load_task_unlocked(task_id)
            task.verify_fencing_token(fencing_token)

            if is_terminal:
                task.lease = None

            self.store._save_task_unlocked(task)

    def submit_execution_result(
        self,
        task_id: str,
        result: ExecutionResult,
        expected_fencing_token: int,
        expected_version: int,
        now: Optional[float] = None
    ) -> TaskEnvelope:
        """Atomically verify state version, lease expiration, and fencing token before accepting results."""
        current_time = time.time() if now is None else now

        with self.store.lock():
            task = self.store._load_task_unlocked(task_id)

            # Optimistic concurrency check
            if task.state_version != expected_version:
                raise PermanentError(
                    "CONCURRENCY_CONFLICT",
                    f"Version conflict on task {task_id}: expected {expected_version}, got {task.state_version}"
                )

            # Phase check
            if task.phase != TaskPhase.RUNNING:
                raise PermanentError(
                    "INVALID_TASK_PHASE",
                    f"Cannot submit result for task {task_id} in phase {task.phase.value}"
                )

            # Fencing token check
            task.verify_fencing_token(expected_fencing_token)
            if result.fencing_token_used != task.fencing_token:
                raise FencingTokenMismatchError(
                    expected_token=task.fencing_token,
                    received_token=result.fencing_token_used,
                    task_id=task_id
                )

            # Lease expiration check
            if not task.lease or task.lease.is_expired(current_time):
                raise LeaseExpiredError(task_id, task.lease.lease_id if task.lease else "NONE")

            # Apply execution result and transition atomically
            task.execution_result = result
            task.lease = None  # Release lease upon successful completion
            task.transition_to(TaskPhase.AWAITING_REVIEW)

            self.store._save_task_unlocked(task)
            return task

    def reconcile_expired_lease(
        self,
        task_id: str,
        is_executor_confirmed_dead_fn: Optional[Callable[[str], bool]] = None,
        now: Optional[float] = None
    ) -> TaskEnvelope:
        """Handle expired lease conservatively under inter-process lock."""
        current_time = time.time() if now is None else now

        with self.store.lock():
            task = self.store._load_task_unlocked(task_id)

            if not task.lease or not task.lease.is_expired(current_time):
                return task

            executor_dead = False
            if is_executor_confirmed_dead_fn is not None and task.assigned_agent:
                try:
                    executor_dead = is_executor_confirmed_dead_fn(task.assigned_agent)
                except Exception:
                    executor_dead = False

            if executor_dead:
                # Executor is provably dead; return to PENDING
                task.lease = None
                task.assigned_agent = None
                task.transition_to(TaskPhase.PENDING, reason="Lease expired and executor confirmed dead")
            else:
                # Executor liveness unverified: QUARANTINE to protect against split-brain
                reason = f"SPLIT_BRAIN_GUARD: Lease expired for agent '{task.assigned_agent}' but death unconfirmed"
                task.transition_to(TaskPhase.QUARANTINED, reason=reason)

            self.store._save_task_unlocked(task)
            return task
