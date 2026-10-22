"""DMAOL Integration Package - Adapters, transport clients, and contract schemas."""
from orchestration.integration.errors import (
    AdapterError,
    ContractValidationError,
    UnauthorizedError,
    StaleGenerationError,
    IdempotencyConflictError,
    AlreadyTerminalError,
    OutcomeUnknownError,
    ResourceIdentityUnverifiableError,
)
from orchestration.integration.contract_messages import (
    CONTRACT_VERSION,
    MAX_CONTROL_MESSAGE_BYTES,
    dump_strict_json,
    parse_strict_json,
    validate_request_message,
    validate_response_message,
    validate_result_envelope,
)
from orchestration.integration.adapter_ledger import AdapterLedger
from orchestration.integration.simulated_transport import SimulatedExecutorTransport
from orchestration.integration.executor_adapter import DmaolExecutorAdapter, canonical_uuid_for_task

__all__ = [
    "AdapterError",
    "ContractValidationError",
    "UnauthorizedError",
    "StaleGenerationError",
    "IdempotencyConflictError",
    "AlreadyTerminalError",
    "OutcomeUnknownError",
    "ResourceIdentityUnverifiableError",
    "CONTRACT_VERSION",
    "MAX_CONTROL_MESSAGE_BYTES",
    "dump_strict_json",
    "parse_strict_json",
    "validate_request_message",
    "validate_response_message",
    "validate_result_envelope",
    "AdapterLedger",
    "SimulatedExecutorTransport",
    "DmaolExecutorAdapter",
    "canonical_uuid_for_task",
]
