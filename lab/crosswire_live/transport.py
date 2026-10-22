"""In-process transport bridging DmaolExecutorAdapter with authentic Forge Link / Ledger."""
from __future__ import annotations

import json
from typing import Any, Union, Optional, Tuple

from orchestration.integration.contract_messages import (
    dump_strict_json,
    parse_strict_json,
)
from orchestration.integration.errors import (
    AdapterError,
    ResourceIdentityUnverifiableError,
)


class ForgeLinkDirectTransport:
    """Delivers raw dmaol-executor/1 JSON control messages directly to Forge Link / ExecutionLedger.handle()."""

    def __init__(self, target_handler: Any):
        if not hasattr(target_handler, "handle") or not callable(getattr(target_handler, "handle")):
            raise TypeError("target_handler must provide a callable handle(raw) method")
        self.target_handler = target_handler

    def send_request(self, raw_request_bytes: bytes) -> bytes:
        """Deliver raw request bytes to the authentic handler and return canonical response bytes."""
        # Forge Link / ExecutionLedger handle() accepts str or bytes
        req_str = raw_request_bytes.decode("utf-8")
        resp = self.target_handler.handle(req_str)

        if isinstance(resp, bytes):
            return resp
        elif isinstance(resp, str):
            return resp.encode("utf-8")
        elif isinstance(resp, dict):
            return dump_strict_json(resp)
        else:
            raise TypeError(f"Unexpected response type from handler: {type(resp)}")

    def get_artifact(self, artifact_ref: str, session_id: Optional[str] = None) -> Tuple[bytes, str]:
        """Retrieve verified artifact (content_bytes, sha) from the authentic authority.

        Ensures that:
        1. The artifact is published in authority storage.
        2. If session_id is provided, the artifact is bound to that session in the authority.
        3. Internal database or lookup exceptions are not silently masked.
        """
        if not hasattr(self.target_handler, "artifact") or not callable(getattr(self.target_handler, "artifact")):
            raise AdapterError(
                "ARTIFACT_AUTHORITY_UNSUPPORTED",
                "Target handler does not support authentic artifact retrieval",
                operation="get_artifact",
            )

        # 1. Authority validation via published_result if available (Adapter)
        if hasattr(self.target_handler, "published_result") and callable(getattr(self.target_handler, "published_result")):
            if session_id is not None:
                try:
                    pub = self.target_handler.published_result(session_id)
                except Exception as exc:
                    raise AdapterError(
                        "ARTIFACT_RETRIEVAL_FAILED",
                        f"Failed to query published result from authority for session '{session_id}': {exc}",
                        operation="get_artifact",
                    ) from exc

                if pub is None:
                    raise AdapterError(
                        "NO_PUBLISHED_RESULT",
                        f"No active published result in authority for session '{session_id}'",
                        operation="get_artifact",
                    )
                if pub.get("artifact_ref") != artifact_ref:
                    raise ResourceIdentityUnverifiableError(
                        f"Artifact '{artifact_ref}' does not match published artifact '{pub.get('artifact_ref')}' for session '{session_id}'"
                    )

        # 2. Authority validation via database binding if available (ExecutionLedger / SQLite backing)
        if hasattr(self.target_handler, "db") and session_id is not None:
            try:
                cur = self.target_handler.db.execute(
                    "SELECT session FROM artifacts WHERE ref = ?",
                    (artifact_ref,)
                )
                row = cur.fetchone()
                if row is None:
                    raise AdapterError(
                        "UNPUBLISHED_ARTIFACT",
                        f"Artifact '{artifact_ref}' is not published in authority storage",
                        operation="get_artifact",
                    )
                bound_session = row[0]
                if bound_session != session_id:
                    raise ResourceIdentityUnverifiableError(
                        f"Artifact '{artifact_ref}' is bound to session '{bound_session}', not expected session '{session_id}'"
                    )
            except (AdapterError, ResourceIdentityUnverifiableError):
                raise
            except Exception as exc:
                raise AdapterError(
                    "ARTIFACT_RETRIEVAL_FAILED",
                    f"Authority database query error for artifact '{artifact_ref}': {exc}",
                    operation="get_artifact",
                ) from exc

        # 3. Retrieve content bytes and hash from authority
        try:
            res = self.target_handler.artifact(artifact_ref)
        except KeyError as exc:
            raise AdapterError(
                "UNPUBLISHED_ARTIFACT",
                f"Artifact '{artifact_ref}' is not published in authority",
                operation="get_artifact",
            ) from exc
        except Exception as exc:
            raise AdapterError(
                "ARTIFACT_RETRIEVAL_FAILED",
                f"Authority error retrieving artifact '{artifact_ref}': {exc}",
                operation="get_artifact",
            ) from exc

        if isinstance(res, tuple) and len(res) >= 2:
            return bytes(res[0]), str(res[1])
        elif isinstance(res, bytes):
            import hashlib
            return res, hashlib.sha256(res).hexdigest()
        else:
            raise AdapterError(
                "ARTIFACT_MALFORMED",
                f"Unexpected artifact format from authority: {type(res)}",
                operation="get_artifact",
            )
