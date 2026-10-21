"""Abstract interfaces and decoupled simulated drivers for DMAOL."""
from abc import ABC, abstractmethod
import time
from typing import Dict, Any, Optional, List
import uuid

from orchestration.models import (
    TaskEnvelope,
    ExecutionResult,
    HumanApproval,
    GitMergeRecord,
    TaskPhase,
    compute_approval_signature,
)
from orchestration.errors import (
    PermanentError,
    FencingTokenMismatchError,
    SecurityViolationError,
)


class SessionLifecycleDriver(ABC):
    """Abstract driver interface decoupling the orchestrator from Executor v3 implementation."""

    @abstractmethod
    def create_session(self, session_id: str) -> str:
        """Provision and launch an isolated agent session."""
        raise NotImplementedError

    @abstractmethod
    def destroy_session(self, session_id: str) -> None:
        """Teardown and clean up session resources."""
        raise NotImplementedError

    @abstractmethod
    def inspect_session(self, session_id: str) -> Dict[str, Any]:
        """Inspect health and status of a session."""
        raise NotImplementedError

    @abstractmethod
    def is_confirmed_dead(self, session_id: str) -> bool:
        """Prove that the session process and container have completely terminated."""
        raise NotImplementedError


class SimulatedSessionDriver(SessionLifecycleDriver):
    """Mock driver for development and tests. Never touches real Docker or kernel."""

    def __init__(self):
        self._sessions: Dict[str, Dict[str, Any]] = {}

    def create_session(self, session_id: str) -> str:
        if session_id in self._sessions and self._sessions[session_id]["status"] == "active":
            raise PermanentError("SESSION_EXISTS", f"Simulated session {session_id} is already active")
        self._sessions[session_id] = {
            "session_id": session_id,
            "status": "active",
            "created_at": time.time(),
            "container_id": f"sim-cid-{uuid.uuid4().hex[:12]}",
        }
        return f"SIMULATED_SESSION_CREATED:{session_id}"

    def destroy_session(self, session_id: str) -> None:
        if session_id in self._sessions:
            self._sessions[session_id]["status"] = "destroyed"
            self._sessions[session_id]["destroyed_at"] = time.time()

    def inspect_session(self, session_id: str) -> Dict[str, Any]:
        if session_id not in self._sessions:
            return {"status": "absent"}
        return self._sessions[session_id]

    def is_confirmed_dead(self, session_id: str) -> bool:
        if session_id not in self._sessions:
            return True
        return self._sessions[session_id].get("status") == "destroyed"


class AgentAdapter(ABC):
    """Abstract interface for dispatching tasks to agents (Codex or Antigravity)."""

    @property
    @abstractmethod
    def agent_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def dispatch_task(self, task: TaskEnvelope, fencing_token: int) -> bool:
        """Dispatch task instructions to the agent with the active fencing token."""
        raise NotImplementedError

    @abstractmethod
    def poll_execution(self, task_id: str) -> Optional[ExecutionResult]:
        """Check if execution has finished and return ExecutionResult."""
        raise NotImplementedError

    @abstractmethod
    def cancel_execution(self, task_id: str) -> None:
        """Request immediate execution cancellation."""
        raise NotImplementedError


class SimulatedAgentAdapter(AgentAdapter):
    """Configurable mock agent adapter for testing lifecycles, retries, and fencing."""

    def __init__(self, agent_id: str):
        self._agent_id = agent_id
        self._dispatched: Dict[str, Dict[str, Any]] = {}
        self._results_to_yield: Dict[str, ExecutionResult] = {}
        self._should_crash_silently: Dict[str, bool] = {}

    @property
    def agent_id(self) -> str:
        return self._agent_id

    def set_result(self, task_id: str, result: ExecutionResult) -> None:
        self._results_to_yield[task_id] = result

    def set_silent_crash(self, task_id: str, crash: bool = True) -> None:
        self._should_crash_silently[task_id] = crash

    def dispatch_task(self, task: TaskEnvelope, fencing_token: int) -> bool:
        self._dispatched[task.task_id] = {
            "task": task,
            "fencing_token": fencing_token,
            "dispatched_at": time.time(),
        }
        return True

    def poll_execution(self, task_id: str) -> Optional[ExecutionResult]:
        if self._should_crash_silently.get(task_id):
            return None
        return self._results_to_yield.get(task_id)

    def cancel_execution(self, task_id: str) -> None:
        if task_id in self._dispatched:
            self._dispatched[task_id]["cancelled"] = True


class HumanGateProvider(ABC):
    """Mandatory cryptographic human gate enforcing authentic operator authorization."""

    @abstractmethod
    def submit_decision(
        self,
        task: TaskEnvelope,
        approver_id: str,
        decision: str,
        comments: str,
        secret_key: bytes
    ) -> HumanApproval:
        raise NotImplementedError

    @abstractmethod
    def verify_decision(
        self,
        task: TaskEnvelope,
        approval: HumanApproval,
        secret_key: bytes
    ) -> bool:
        raise NotImplementedError


class CryptographicHumanGate(HumanGateProvider):
    """Cryptographically authentic human approval gate using HMAC-SHA256 signatures."""

    def submit_decision(
        self,
        task: TaskEnvelope,
        approver_id: str,
        decision: str,
        comments: str,
        secret_key: bytes
    ) -> HumanApproval:
        if not approver_id or approver_id.startswith("agent-"):
            raise SecurityViolationError(
                "AGENT_CANNOT_SELF_APPROVE",
                f"Approver '{approver_id}' is not an authorized human operator"
            )
        if decision not in ("APPROVED", "REJECTED"):
            raise PermanentError("INVALID_DECISION", f"Decision must be APPROVED or REJECTED, got '{decision}'")
        if not secret_key or len(secret_key) < 16:
            raise SecurityViolationError("INVALID_OPERATOR_KEY", "Operator secret key must be at least 16 bytes")

        now = time.time()
        token = f"auth-token-{uuid.uuid4().hex[:16]}"
        signature = compute_approval_signature(approver_id, task.task_id, decision, now, secret_key)

        return HumanApproval(
            approver_id=approver_id,
            decision=decision,
            approval_token=token,
            comments=comments,
            timestamp=now,
            signature=signature
        )

    def verify_decision(
        self,
        task: TaskEnvelope,
        approval: HumanApproval,
        secret_key: bytes
    ) -> bool:
        if not approval.verify_signature(task.task_id, secret_key):
            raise SecurityViolationError(
                "FORGED_HUMAN_APPROVAL",
                f"Approval signature verification failed for task '{task.task_id}' by '{approval.approver_id}'"
            )
        return True
