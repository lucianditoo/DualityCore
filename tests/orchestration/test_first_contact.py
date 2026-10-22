"""DUALITY CORE - Pruebas automatizadas de Integracion E2E: Operacion FIRST CONTACT."""
import tempfile
import unittest

from orchestration.demo import run_first_contact_demo
from orchestration.models import TaskPhase


class TestFirstContactE2E(unittest.TestCase):
    """Verifica la ejecucion completa y reproducible de la Operacion FIRST CONTACT."""

    def test_run_first_contact_demo(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report = run_first_contact_demo(temp_dir, verbose=False)

            # 1. Verificar tareas completadas para ambos agentes simulados
            completed = report["tasks_completed"]
            self.assertEqual(len(completed), 2)
            agents = {agent for agent, _ in completed}
            self.assertIn("codex-sim", agents)
            self.assertIn("antigravity-sim", agents)

            # 2. Verificar flujos de rechazo evaluados
            rejections = report["rejections_tested"]
            rejection_types = {r_type for r_type, _, _ in rejections}
            self.assertIn("rework", rejection_types)
            self.assertIn("cancelled", rejection_types)

            # 3. Verificar pruebas negativas superadas
            neg_passed = set(report["negative_tests_passed"])
            expected_negatives = {
                "IdempotencyConflictError",
                "FencingTokenMismatchError",
                "ResourceConflictError",
                "AGENT_CANNOT_SELF_APPROVE",
                "FORGED_HUMAN_APPROVAL",
                "AWAITING_REVIEW_TO_FAILED_ILLEGAL",
                "PersistenceRecoveryVerified",
            }
            for expected in expected_negatives:
                self.assertIn(expected, neg_passed, f"Falto prueba negativa: {expected}")

            # 4. Verificar transiciones de estado observadas en la ejecucion
            transitions = report["transitions_observed"]
            self.assertTrue(len(transitions) >= 14)

            # Verificar que existio ciclo de aprobacion y merge
            phase_to_pairs = {(from_p, to_p) for _, from_p, to_p in transitions}
            self.assertIn((TaskPhase.RUNNING.value, TaskPhase.AWAITING_REVIEW.value), phase_to_pairs)
            self.assertIn((TaskPhase.AWAITING_REVIEW.value, TaskPhase.APPROVED.value), phase_to_pairs)
            self.assertIn((TaskPhase.APPROVED.value, TaskPhase.MERGING.value), phase_to_pairs)
            self.assertIn((TaskPhase.MERGING.value, TaskPhase.COMPLETED.value), phase_to_pairs)
            self.assertIn((TaskPhase.AWAITING_REVIEW.value, TaskPhase.PENDING.value), phase_to_pairs)
            self.assertIn((TaskPhase.AWAITING_REVIEW.value, TaskPhase.CANCELLING.value), phase_to_pairs)


if __name__ == "__main__":
    unittest.main()
