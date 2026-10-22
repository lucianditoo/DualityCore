"""In-process transport bridging DmaolExecutorAdapter with authentic Forge Link / Ledger."""
import json
from typing import Any, Union

from orchestration.integration.contract_messages import (
    dump_strict_json,
    parse_strict_json,
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

    def get_artifact(self, artifact_ref: str) -> Optional[bytes]:
        """Retrieve verified artifact bytes from the target authority if supported."""
        if hasattr(self.target_handler, "artifact") and callable(getattr(self.target_handler, "artifact")):
            try:
                res = self.target_handler.artifact(artifact_ref)
                if isinstance(res, tuple):
                    return res[0]  # (content, sha)
                elif isinstance(res, bytes):
                    return res
            except Exception:
                return None
        return None
