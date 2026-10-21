"""Durable persistence and conservative crash recovery for Duality Orchestration."""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Dict, Any, List, Optional, Tuple

from orchestration.errors import (
    PermanentError,
    SecurityViolationError,
    QuarantineRequiredError,
    IdempotencyConflictError,
)
from orchestration.models import TaskEnvelope, TaskPhase
from orchestration.lock import CrossPlatformFileLock

MAX_RECORD_BYTES = 65536


def _unique_object(pairs):
    res = {}
    for k, v in pairs:
        if k in res:
            raise PermanentError("DUPLICATE_JSON_KEY", f"Duplicate key '{k}' in JSON payload")
        res[k] = v
    return res


def decode_canonical_json(raw_bytes: bytes) -> Dict[str, Any]:
    """Strict JSON parser rejecting duplicates, non-finite floats, and invalid encodings."""
    try:
        text = raw_bytes.decode('utf-8')
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=lambda c: (_ for _ in ()).throw(PermanentError("NONFINITE_JSON_NUMBER", f"Constant '{c}' invalid"))
        )
    except Exception as e:
        if isinstance(e, PermanentError):
            raise
        raise PermanentError("JSON_DECODE_ERROR", str(e)) from e


def canonical_bytes(obj: Any) -> bytes:
    """Encode an object into deterministic canonical JSON bytes."""
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


class TaskStore:
    """Durable file-based store for task envelopes with inter-process locking and crash recovery."""

    def __init__(self, root_dir: str):
        self.root = Path(root_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.tasks_dir = self.root / "tasks"
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        self.idempotency_dir = self.root / "idempotency"
        self.idempotency_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = str(self.root / ".store.lock")
        self._is_posix = (os.name == 'posix')

    def lock(self) -> CrossPlatformFileLock:
        """Return an inter-process file lock for the store."""
        return CrossPlatformFileLock(self.lock_file)

    def _sync_dir(self, directory: Path) -> None:
        """Perform fsync on directory inode on POSIX platforms to commit directory entry mutations."""
        if self._is_posix:
            try:
                dfd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass

    def save_task(self, task: TaskEnvelope, expected_version: Optional[int] = None) -> None:
        """Persist a task envelope atomically under the inter-process store lock."""
        with self.lock():
            self._save_task_unlocked(task, expected_version=expected_version)

    def _save_task_unlocked(self, task: TaskEnvelope, expected_version: Optional[int] = None) -> None:
        """Internal save implementation executed with store lock already held."""
        task_file = self.tasks_dir / f"{task.task_id}.json"

        # Optimistic concurrency check
        if task_file.exists():
            existing = self._load_task_unlocked(task.task_id)
            if expected_version is not None and existing.state_version != expected_version:
                raise PermanentError(
                    "CONCURRENCY_CONFLICT",
                    f"Task {task.task_id} state version mismatch: expected {expected_version}, got {existing.state_version}"
                )

        # 1. Pre-validate and write idempotency mapping FIRST under the lock
        self._record_idempotency_unlocked(task.idempotency_key, task.task_id, task.payload_digest)

        # 2. Prepare task payload with digest envelope
        data = task.to_dict()
        data_bytes = canonical_bytes(data)
        digest = hashlib.sha256(data_bytes).hexdigest()
        envelope = {
            "schema_version": 1,
            "digest": digest,
            "record": data
        }
        envelope_bytes = canonical_bytes(envelope) + b"\n"

        if len(envelope_bytes) > MAX_RECORD_BYTES:
            raise PermanentError("RECORD_TOO_LARGE", f"Task record exceeds maximum size of {MAX_RECORD_BYTES} bytes")

        # Atomic write pattern: temp file -> fsync -> rename -> sync directory
        tmp_name = f".tmp-{task.task_id}-{time.time_ns()}.json"
        tmp_file = self.tasks_dir / tmp_name

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, 'O_CLOEXEC'):
            flags |= os.O_CLOEXEC

        mode = 0o600 if self._is_posix else 0o666
        fd = os.open(str(tmp_file), flags, mode)
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(envelope_bytes)
                f.flush()
                os.fsync(f.fileno())
            os.replace(str(tmp_file), str(task_file))
            self._sync_dir(self.tasks_dir)
        except Exception:
            if tmp_file.exists():
                try:
                    tmp_file.unlink()
                except OSError:
                    pass
            raise

    def load_task(self, task_id: str) -> TaskEnvelope:
        """Load and verify a task envelope by ID under the store lock."""
        with self.lock():
            return self._load_task_unlocked(task_id)

    def _load_task_unlocked(self, task_id: str) -> TaskEnvelope:
        task_file = self.tasks_dir / f"{task_id}.json"
        if not task_file.exists():
            raise PermanentError("TASK_NOT_FOUND", f"Task {task_id} not found in store")

        raw_bytes = task_file.read_bytes()
        envelope = decode_canonical_json(raw_bytes)

        if not isinstance(envelope, dict) or set(envelope.keys()) != {"schema_version", "digest", "record"}:
            raise PermanentError("INVALID_ENVELOPE", f"Corrupt envelope structure in task {task_id}")

        expected_digest = envelope["digest"]
        record = envelope["record"]
        actual_digest = hashlib.sha256(canonical_bytes(record)).hexdigest()

        if actual_digest != expected_digest:
            raise PermanentError(
                "INTEGRITY_CHECK_FAILED",
                f"Digest mismatch for task {task_id}: expected {expected_digest}, computed {actual_digest}"
            )

        return TaskEnvelope.from_dict(record)

    def _record_idempotency_unlocked(self, idempotency_key: str, task_id: str, payload_digest: str) -> None:
        """Record idempotency mapping ensuring identical key maps to identical payload."""
        safe_key = hashlib.sha256(idempotency_key.encode('utf-8')).hexdigest()
        key_file = self.idempotency_dir / f"{safe_key}.json"

        if key_file.exists():
            data = decode_canonical_json(key_file.read_bytes())
            if data.get("payload_digest") != payload_digest:
                raise IdempotencyConflictError(idempotency_key)
            return

        record = {
            "idempotency_key": idempotency_key,
            "task_id": task_id,
            "payload_digest": payload_digest,
            "created_at": time.time(),
        }
        content = canonical_bytes(record)
        tmp = self.idempotency_dir / f".tmp-{safe_key}-{time.time_ns()}.json"
        
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, 'O_CLOEXEC'):
            flags |= os.O_CLOEXEC
        mode = 0o600 if self._is_posix else 0o666

        fd = os.open(str(tmp), flags, mode)
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(str(tmp), str(key_file))
            self._sync_dir(self.idempotency_dir)
        except Exception:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
            raise

    def find_by_idempotency_key(self, idempotency_key: str) -> Optional[TaskEnvelope]:
        """Look up existing task by idempotency key under store lock."""
        with self.lock():
            safe_key = hashlib.sha256(idempotency_key.encode('utf-8')).hexdigest()
            key_file = self.idempotency_dir / f"{safe_key}.json"
            if not key_file.exists():
                return None
            data = decode_canonical_json(key_file.read_bytes())
            return self._load_task_unlocked(data["task_id"])

    def _list_all_tasks_unlocked(self) -> List[TaskEnvelope]:
        tasks = []
        for p in self.tasks_dir.glob("*.json"):
            if p.name.startswith(".tmp-"):
                continue
            task_id = p.stem
            try:
                tasks.append(self._load_task_unlocked(task_id))
            except Exception:
                continue
        return tasks

    def list_all_tasks(self) -> List[TaskEnvelope]:
        """List all tasks stored under lock."""
        with self.lock():
            return self._list_all_tasks_unlocked()

    def recover_on_startup(self) -> Tuple[List[str], List[str]]:
        """Conservative crash recovery scanner with transactional reconciliation."""
        with self.lock():
            recovered = []
            quarantined = []

            # 1. Clean dangling temp files in tasks and idempotency dirs
            for p in self.tasks_dir.glob(".tmp-*"):
                try:
                    p.unlink()
                except OSError:
                    pass
            for p in self.idempotency_dir.glob(".tmp-*"):
                try:
                    p.unlink()
                except OSError:
                    pass

            # 2. Reconcile idempotency entries with tasks
            existing_task_ids = set()
            for p in self.tasks_dir.glob("*.json"):
                if not p.name.startswith(".tmp-"):
                    existing_task_ids.add(p.stem)

            # Reconcile idempotency index: remove dangling keys referencing non-existent tasks
            for p in self.idempotency_dir.glob("*.json"):
                if p.name.startswith(".tmp-"):
                    continue
                try:
                    data = decode_canonical_json(p.read_bytes())
                    target_task_id = data.get("task_id")
                    if target_task_id not in existing_task_ids:
                        p.unlink()  # Dangling idempotency mapping removed
                except Exception:
                    try:
                        p.unlink()
                    except OSError:
                        pass

            # 3. Scan and reconcile tasks
            for task_id in sorted(existing_task_ids):
                try:
                    task = self._load_task_unlocked(task_id)
                except Exception as e:
                    quarantined.append(f"{task_id}:CORRUPT_RECORD:{str(e)}")
                    continue

                # Ensure idempotency index is reconstructed if missing (crash between task & index write)
                safe_key = hashlib.sha256(task.idempotency_key.encode('utf-8')).hexdigest()
                key_file = self.idempotency_dir / f"{safe_key}.json"
                if not key_file.exists():
                    self._record_idempotency_unlocked(task.idempotency_key, task.task_id, task.payload_digest)

                # Check ambiguous in-flight phases
                if task.phase in (TaskPhase.RUNNING, TaskPhase.CANCELLING, TaskPhase.MERGING):
                    reason = f"CRASH_RECOVERY: Task found in mid-flight phase {task.phase.value} on startup"
                    try:
                        task.transition_to(TaskPhase.QUARANTINED, reason=reason)
                        self._save_task_unlocked(task)
                        quarantined.append(f"{task_id}:{reason}")
                    except Exception as ex:
                        quarantined.append(f"{task_id}:TRANSITION_FAILED:{str(ex)}")
                elif task.phase == TaskPhase.ASSIGNED and task.lease and task.lease.is_expired():
                    reason = "CRASH_RECOVERY: Lease expired during system downtime; previous executor liveness unverified"
                    task.transition_to(TaskPhase.QUARANTINED, reason=reason)
                    self._save_task_unlocked(task)
                    quarantined.append(f"{task_id}:{reason}")
                else:
                    recovered.append(task_id)

            return recovered, quarantined
