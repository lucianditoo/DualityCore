"""Simulated transport client and server implementing dmaol-executor/1 operations."""
import hashlib
import time
from typing import Dict, Any, Optional
import uuid

from orchestration.integration.errors import (
    AdapterError,
    ContractValidationError,
    OutcomeUnknownError,
)
from orchestration.integration.contract_messages import (
    dump_strict_json,
    parse_strict_json,
    validate_request_message,
    validate_response_message,
    validate_result_envelope,
)
from orchestration.integration.adapter_ledger import AdapterLedger


class SimulatedExecutorTransport:
    """Simulated transport client and server communicating via dmaol-executor/1 JSON control messages."""

    def __init__(self, ledger: AdapterLedger):
        self.ledger = ledger
        self.drop_next_ack: bool = False
        self.crash_before_ack: bool = False

    def send_request(self, raw_request_bytes: bytes) -> bytes:
        """Process incoming raw JSON request message and return raw JSON response bytes."""
        parsed_req = parse_strict_json(raw_request_bytes)
        validated_req = validate_request_message(parsed_req)

        op = validated_req["op"]

        if self.crash_before_ack:
            # Simulate crash: write intent in ledger, then raise OutcomeUnknownError
            if op == "start":
                self.ledger.record_start_intent(validated_req)
            self.crash_before_ack = False
            raise OutcomeUnknownError(
                "Simulated crash between intention and response delivery",
                operation=op,
                executor_error_code="TRANSPORT_CRASH_BEFORE_ACK"
            )

        if op == "start":
            response_dict = self.ledger.record_start_intent(validated_req)
        elif op == "observe":
            response_dict = self.ledger.observe(validated_req)
        elif op == "cancel":
            response_dict = self.ledger.cancel(validated_req)
        elif op == "recover":
            # Recover performs non-destructive observe and reconciliation
            response_dict = self.ledger.observe(validated_req)
        else:
            raise ContractValidationError(f"Unsupported operation: {op}")

        validate_response_message(response_dict)

        if self.drop_next_ack:
            self.drop_next_ack = False
            raise OutcomeUnknownError(
                "Network timeout: response lost in transit (lost ACK)",
                operation=op,
                details={"request_id": validated_req["request_id"]}
            )

        return dump_strict_json(response_dict)

    def simulate_executor_lifecycle_progress(
        self,
        task_id: str,
        expected_version: int,
        target_task_state: str = "RUNNING",
        executor_instance: Optional[str] = None
    ) -> Dict[str, Any]:
        """Simulate executor driver launching container and acknowledging dispatch."""
        instance = executor_instance or hashlib.sha256(f"inst-{task_id}".encode()).hexdigest()[:32]
        # Transition to ACTIVE_OBSERVED and RUNNING
        updated = self.ledger.update_resource_observation(
            task_id=task_id,
            expected_version=expected_version,
            resource_state="ACTIVE_OBSERVED",
            task_state=target_task_state,
            certainty="CONFIRMED",
            executor_instance=instance
        )
        return updated

    def dispatch_execution_result(self, raw_result_bytes: bytes) -> bytes:
        """Process and validate an incoming execution result envelope."""
        parsed = parse_strict_json(raw_result_bytes)
        validated = validate_result_envelope(parsed)
        accepted = self.ledger.record_execution_result(validated)
        return dump_strict_json(accepted)
