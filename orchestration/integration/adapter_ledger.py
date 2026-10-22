"""Durable ledger for DMAOL-Executor Adapter with atomic writes, OCC, and idempotency."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Dict, Any, Optional, Tuple
import uuid

from orchestration.lock import CrossPlatformFileLock
from orchestration.integration.errors import (
    AdapterError,
    ContractValidationError,
    StaleGenerationError,
    IdempotencyConflictError,
    AlreadyTerminalError,
    ResourceIdentityUnverifiableError,
)
from orchestration.integration.contract_messages import (
    CONTRACT_VERSION,
    dump_strict_json,
    parse_strict_json,
    validate_response_message,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AdapterLedger:
    """Durable file-backed ledger recording adapter operations, active tuples, and tombstones."""

    def __init__(self, root_dir: str):
        self.root = Path(root_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.ops_dir = self.root / "operations"
        self.ops_dir.mkdir(parents=True, exist_ok=True)
        self.tuples_dir = self.root / "active_tuples"
        self.tuples_dir.mkdir(parents=True, exist_ok=True)
        self.tombstones_dir = self.root / "tombstones"
        self.tombstones_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir = self.root / "results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = str(self.root / ".ledger.lock")

    def lock(self) -> CrossPlatformFileLock:
        return CrossPlatformFileLock(self.lock_file)

    def _sync_dir(self, directory: Path) -> None:
        if os.name == "posix":
            try:
                dfd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass

    def _atomic_write_file(self, target_path: Path, data_bytes: bytes) -> None:
        tmp_name = f".tmp-{uuid.uuid4().hex}-{time.time_ns()}.json"
        tmp_file = target_path.parent / tmp_name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY

        fd = os.open(str(tmp_file), flags, 0o600)
        try:
            os.write(fd, data_bytes)
            os.fsync(fd)
        finally:
            os.close(fd)

        os.replace(str(tmp_file), str(target_path))
        self._sync_dir(target_path.parent)

    def _read_file_strict(self, path: Path) -> Dict[str, Any]:
        with open(path, "rb") as f:
            raw = f.read()
        return parse_strict_json(raw)

    def record_start_intent(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Record intent to start a task attempt under kernel lock with idempotency and fencing checks."""
        req_id = req["request_id"]
        task_id = req["task_id"]
        attempt_id = req["attempt_id"]
        generation = req["generation"]
        deployment_id = req["deployment_id"]

        with self.lock():
            # 1. Idempotency Check by request_id
            op_file = self.ops_dir / f"{req_id}.json"
            req_bytes = dump_strict_json(req)
            req_hash = hashlib.sha256(req_bytes).hexdigest()

            if op_file.exists():
                existing_op = self._read_file_strict(op_file)
                if existing_op.get("request_hash") == req_hash:
                    return existing_op["response"]
                raise IdempotencyConflictError(
                    f"request_id '{req_id}' was already submitted with different parameters"
                )

            # 2. Check Tombstones
            tombstone_file = self.tombstones_dir / f"{task_id}_{attempt_id}.json"
            if tombstone_file.exists():
                raise ContractValidationError(
                    f"Attempt '{attempt_id}' for task '{task_id}' has already been tombstoned and cannot be reused"
                )

            # 3. Check Active Tuple Uniqueness and Generation Authority
            tuple_file = self.tuples_dir / f"{task_id}.json"
            if tuple_file.exists():
                active_tuple = self._read_file_strict(tuple_file)
                active_gen = active_tuple["generation"]
                if generation <= active_gen:
                    raise StaleGenerationError(
                        f"Request generation {generation} is stale; active generation is {active_gen}"
                    )
                # If older generation active, archive it to tombstone
                self._archive_tuple_unlocked(active_tuple, reason="SUPERSEDED_BY_NEWER_GENERATION")

            # 4. Allocate unique session_id (s- + 32 lowercase hex)
            session_id = f"s-{uuid.uuid4().hex}"
            operation_id = str(uuid.uuid4())

            new_tuple = {
                "deployment_id": deployment_id,
                "task_id": task_id,
                "attempt_id": attempt_id,
                "generation": generation,
                "session_id": session_id,
                "executor_instance": None,
                "state_version": 1,
                "task_state": "ACCEPTED",
                "resource_state": "NOT_CREATED",
                "certainty": "CONFIRMED",
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
            }

            response_data = {
                "version": CONTRACT_VERSION,
                "request_id": req_id,
                "operation_id": operation_id,
                "deployment_id": deployment_id,
                "task_id": task_id,
                "attempt_id": attempt_id,
                "generation": generation,
                "session_id": session_id,
                "executor_instance": None,
                "state_version": 1,
                "task_state": "ACCEPTED",
                "resource_state": "NOT_CREATED",
                "certainty": "CONFIRMED",
                "observed_at": _now_iso(),
                "source": "adapter-ledger",
                "error": None,
            }
            validate_response_message(response_data)

            # Persist active tuple and operation record atomically
            self._atomic_write_file(tuple_file, dump_strict_json(new_tuple))

            op_record = {
                "request_id": req_id,
                "request_hash": req_hash,
                "operation_id": operation_id,
                "op": "start",
                "task_id": task_id,
                "attempt_id": attempt_id,
                "generation": generation,
                "session_id": session_id,
                "response": response_data,
                "created_at": _now_iso(),
            }
            self._atomic_write_file(op_file, dump_strict_json(op_record))
            return response_data

    def observe(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Query state of an operation or bound tuple under kernel lock."""
        req_id = req["request_id"]
        with self.lock():
            # Check if this observe request was already recorded
            op_file = self.ops_dir / f"{req_id}.json"
            req_bytes = dump_strict_json(req)
            req_hash = hashlib.sha256(req_bytes).hexdigest()
            if op_file.exists():
                existing = self._read_file_strict(op_file)
                if existing.get("request_hash") == req_hash:
                    return existing["response"]
                raise IdempotencyConflictError(f"Observe request_id '{req_id}' used with conflicting payload")

            # Lost ACK lookup by lookup_request_id
            if "lookup_request_id" in req:
                target_req_id = req["lookup_request_id"]
                target_op_file = self.ops_dir / f"{target_req_id}.json"
                if not target_op_file.exists():
                    raise ContractValidationError(f"Original operation '{target_req_id}' not found")
                target_op = self._read_file_strict(target_op_file)
                resp = target_op["response"]
                # Record this observe query
                self._record_query_op_unlocked(req_id, req_hash, "observe", resp)
                return resp

            # Full tuple lookup
            task_id = req["task_id"]
            tuple_file = self.tuples_dir / f"{task_id}.json"
            if not tuple_file.exists():
                raise ResourceIdentityUnverifiableError(f"No active tuple found for task '{task_id}'")

            active = self._read_file_strict(tuple_file)
            self._validate_tuple_match_unlocked(req, active)

            response_data = {
                "version": CONTRACT_VERSION,
                "request_id": req_id,
                "operation_id": str(uuid.uuid4()),
                "deployment_id": active["deployment_id"],
                "task_id": active["task_id"],
                "attempt_id": active["attempt_id"],
                "generation": active["generation"],
                "session_id": active["session_id"],
                "executor_instance": active["executor_instance"],
                "state_version": active["state_version"],
                "task_state": active["task_state"],
                "resource_state": active["resource_state"],
                "certainty": active["certainty"],
                "observed_at": _now_iso(),
                "source": "adapter-ledger",
                "error": None,
            }
            validate_response_message(response_data)
            self._record_query_op_unlocked(req_id, req_hash, "observe", response_data)
            return response_data

    def cancel(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Request cancellation of an active task attempt under kernel lock."""
        req_id = req["request_id"]
        task_id = req["task_id"]

        with self.lock():
            op_file = self.ops_dir / f"{req_id}.json"
            req_bytes = dump_strict_json(req)
            req_hash = hashlib.sha256(req_bytes).hexdigest()
            if op_file.exists():
                existing = self._read_file_strict(op_file)
                if existing.get("request_hash") == req_hash:
                    return existing["response"]
                raise IdempotencyConflictError(f"Cancel request_id '{req_id}' used with conflicting payload")

            tuple_file = self.tuples_dir / f"{task_id}.json"
            if not tuple_file.exists():
                raise ResourceIdentityUnverifiableError(f"No active tuple found for task '{task_id}'")

            active = self._read_file_strict(tuple_file)
            self._validate_tuple_match_unlocked(req, active)

            # Check terminal states
            if active["task_state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                raise AlreadyTerminalError(
                    f"Cannot cancel: task '{task_id}' is already terminal ({active['task_state']})"
                )

            # Update tuple to CANCEL_REQUESTED and STOPPING
            active["task_state"] = "CANCEL_REQUESTED"
            active["resource_state"] = "STOPPING"
            active["state_version"] += 1
            active["updated_at"] = _now_iso()
            self._atomic_write_file(tuple_file, dump_strict_json(active))

            response_data = {
                "version": CONTRACT_VERSION,
                "request_id": req_id,
                "operation_id": str(uuid.uuid4()),
                "deployment_id": active["deployment_id"],
                "task_id": active["task_id"],
                "attempt_id": active["attempt_id"],
                "generation": active["generation"],
                "session_id": active["session_id"],
                "executor_instance": active["executor_instance"],
                "state_version": active["state_version"],
                "task_state": active["task_state"],
                "resource_state": active["resource_state"],
                "certainty": active["certainty"],
                "observed_at": _now_iso(),
                "source": "adapter-ledger",
                "error": None,
            }
            validate_response_message(response_data)
            self._record_query_op_unlocked(req_id, req_hash, "cancel", response_data)
            return response_data

    def update_resource_observation(
        self,
        task_id: str,
        expected_version: int,
        resource_state: str,
        task_state: Optional[str] = None,
        certainty: str = "OBSERVED",
        executor_instance: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update live resource observation from executor driver under kernel lock."""
        with self.lock():
            tuple_file = self.tuples_dir / f"{task_id}.json"
            if not tuple_file.exists():
                raise ResourceIdentityUnverifiableError(f"No active tuple found for task '{task_id}'")

            active = self._read_file_strict(tuple_file)
            if active["state_version"] != expected_version:
                raise AdapterError(
                    "CONCURRENCY_CONFLICT",
                    f"State version mismatch for task '{task_id}': expected {expected_version}, got {active['state_version']}"
                )

            active["resource_state"] = resource_state
            if task_state:
                active["task_state"] = task_state
            if executor_instance:
                active["executor_instance"] = executor_instance
            active["certainty"] = certainty
            active["state_version"] += 1
            active["updated_at"] = _now_iso()

            self._atomic_write_file(tuple_file, dump_strict_json(active))
            return active

    def record_execution_result(self, result_env: Dict[str, Any]) -> Dict[str, Any]:
        """Atomically record and validate an incoming execution result envelope."""
        task_id = result_env["task_id"]
        attempt_id = result_env["attempt_id"]
        generation = result_env["generation"]
        session_id = result_env["session_id"]
        result_id = result_env["result_id"]

        with self.lock():
            # Check if this exact result_id was already accepted
            result_file = self.results_dir / f"{result_id}.json"
            res_bytes = dump_strict_json(result_env)
            res_hash = hashlib.sha256(res_bytes).hexdigest()

            if result_file.exists():
                existing = self._read_file_strict(result_file)
                if existing.get("result_hash") == res_hash:
                    return existing["result"]
                raise IdempotencyConflictError(f"result_id '{result_id}' already exists with different payload")

            # Check active tuple
            tuple_file = self.tuples_dir / f"{task_id}.json"
            if not tuple_file.exists():
                # Archived or missing; preserve as segregated evidence
                self._preserve_segregated_evidence_unlocked(result_env, reason="TUPLE_ABSENT")
                raise StaleGenerationError(f"No active tuple for task '{task_id}' to accept result")

            active = self._read_file_strict(tuple_file)

            # Strict generation check
            if generation < active["generation"]:
                self._preserve_segregated_evidence_unlocked(result_env, reason="STALE_GENERATION")
                raise StaleGenerationError(
                    f"Result generation {generation} is stale (active is {active['generation']})"
                )
            if generation > active["generation"]:
                raise ContractValidationError(
                    f"Result generation {generation} exceeds active generation {active['generation']}"
                )

            # Strict attempt and session match
            if attempt_id != active["attempt_id"]:
                self._preserve_segregated_evidence_unlocked(result_env, reason="ATTEMPT_MISMATCH")
                raise ContractValidationError(
                    f"Result attempt_id '{attempt_id}' does not match active attempt '{active['attempt_id']}'"
                )
            if session_id != active["session_id"]:
                self._preserve_segregated_evidence_unlocked(result_env, reason="SESSION_MISMATCH")
                raise ResourceIdentityUnverifiableError(
                    f"Result session_id '{session_id}' does not match active session '{active['session_id']}'"
                )

            if active["task_state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                raise AlreadyTerminalError(f"Task '{task_id}' is already terminal ({active['task_state']})")

            # Mark active tuple as SUCCEEDED or FAILED
            final_task_state = "SUCCEEDED" if result_env["outcome"] == "SUCCEEDED" else "FAILED"
            active["task_state"] = final_task_state
            active["resource_state"] = "ACTIVE_OBSERVED"
            active["certainty"] = "CONFIRMED"
            active["state_version"] += 1
            active["updated_at"] = _now_iso()

            self._atomic_write_file(tuple_file, dump_strict_json(active))

            # Store result record
            record = {
                "result_id": result_id,
                "result_hash": res_hash,
                "task_id": task_id,
                "attempt_id": attempt_id,
                "generation": generation,
                "session_id": session_id,
                "result": result_env,
                "accepted_at": _now_iso(),
            }
            self._atomic_write_file(result_file, dump_strict_json(record))
            return result_env

    def _validate_tuple_match_unlocked(self, req: Dict[str, Any], active: Dict[str, Any]) -> None:
        if req["generation"] < active["generation"]:
            raise StaleGenerationError(
                f"Request generation {req['generation']} is stale (active is {active['generation']})"
            )
        if req["generation"] != active["generation"]:
            raise StaleGenerationError(
                f"Generation mismatch: expected {active['generation']}, got {req['generation']}"
            )
        if req["attempt_id"] != active["attempt_id"]:
            raise ContractValidationError(
                f"attempt_id mismatch: expected '{active['attempt_id']}', got '{req['attempt_id']}'"
            )
        if req["session_id"] != active["session_id"]:
            raise ResourceIdentityUnverifiableError(
                f"session_id mismatch: expected '{active['session_id']}', got '{req['session_id']}'"
            )
        if req["deployment_id"] != active["deployment_id"]:
            raise ContractValidationError(
                f"deployment_id mismatch: expected '{active['deployment_id']}', got '{req['deployment_id']}'"
            )

    def _archive_tuple_unlocked(self, tuple_data: Dict[str, Any], reason: str) -> None:
        task_id = tuple_data["task_id"]
        attempt_id = tuple_data["attempt_id"]
        tombstone_file = self.tombstones_dir / f"{task_id}_{attempt_id}.json"
        archive_data = dict(tuple_data)
        archive_data["tombstone_reason"] = reason
        archive_data["tombstoned_at"] = _now_iso()
        self._atomic_write_file(tombstone_file, dump_strict_json(archive_data))

    def _preserve_segregated_evidence_unlocked(self, result_env: Dict[str, Any], reason: str) -> None:
        ev_id = f"ev-{uuid.uuid4().hex}"
        ev_file = self.results_dir / f"segregated_{ev_id}.json"
        record = {
            "evidence_id": ev_id,
            "rejection_reason": reason,
            "envelope": result_env,
            "recorded_at": _now_iso(),
        }
        self._atomic_write_file(ev_file, dump_strict_json(record))

    def _record_query_op_unlocked(
        self, req_id: str, req_hash: str, op: str, response: Dict[str, Any]
    ) -> None:
        op_file = self.ops_dir / f"{req_id}.json"
        record = {
            "request_id": req_id,
            "request_hash": req_hash,
            "operation_id": response["operation_id"],
            "op": op,
            "response": response,
            "created_at": _now_iso(),
        }
        self._atomic_write_file(op_file, dump_strict_json(record))
