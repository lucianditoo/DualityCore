"""Backward-compatible offline simulator facade over the shared authority."""
from .ledger import (  # Re-export the original public validation API.
    VERSION, MAX_INT, TASKS, RESOURCES, CERTAINTIES, TERMINAL, TUPLE, COMMON,
    START, InvalidRequest, LedgerCorrupt, canonical, uid, integer, token,
    reference, decode, validate, validate_ledger, Ledger,
)


class Simulator(Ledger):
    """Unchanged offline API and wire responses; never launches a process."""

    def __init__(self, ledger, deployment_id, *, simulated_channel=False):
        super().__init__(ledger, deployment_id, simulated_channel=simulated_channel)
