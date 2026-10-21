"""Unit tests for GitPolicy, protected branch blocking, and cross-platform checks."""
import unittest

from orchestration.errors import SecurityViolationError
from orchestration.git_policy import GitPolicy


class TestGitPolicy(unittest.TestCase):

    def test_protected_branches_strictly_blocked(self):
        blocked_targets = [
            "main", "master", "production", "prod", "stable",
            "release/v1.0", "releases/2026.1", "hotfix/urgent", "infra/terraform"
        ]
        for branch in blocked_targets:
            with self.subTest(branch=branch):
                with self.assertRaises(SecurityViolationError) as ctx:
                    GitPolicy.validate_target_branch(branch, allow_protected=False)
                self.assertIn("PROTECTED_BRANCH_MODIFICATION_BLOCKED", str(ctx.exception))

    def test_allow_protected_only_for_human_authorized_operations(self):
        # When explicitly authorized by the human gate
        validated = GitPolicy.validate_target_branch("main", allow_protected=True)
        self.assertEqual(validated, "main")

    def test_agent_branch_naming_conventions(self):
        valid_branches = [
            "duality/task-123/codex",
            "agent/antigravity/task-456",
            "duality/refactor-auth/agent-01"
        ]
        for branch in valid_branches:
            with self.subTest(branch=branch):
                self.assertEqual(GitPolicy.validate_agent_branch(branch), branch)

        invalid_branches = [
            "main",
            "feature-without-prefix",
            "random/branch/with/too/many/slashes",
            "duality//bad",
            "../escape",
        ]
        for branch in invalid_branches:
            with self.subTest(branch=branch):
                with self.assertRaises(SecurityViolationError):
                    GitPolicy.validate_agent_branch(branch)

    def test_windows_reserved_names_blocked(self):
        bad_paths = [
            ["src/CON.py"],
            ["utils/prn/helper.js"],
            ["data/NUL"],
            ["com1.txt"],
            ["nested/aux.json"],
        ]
        for paths in bad_paths:
            with self.subTest(paths=paths):
                with self.assertRaises(SecurityViolationError) as ctx:
                    GitPolicy.validate_cross_platform_paths(paths)
                self.assertIn("WINDOWS_RESERVED_FILENAME", str(ctx.exception))

    def test_case_collision_detection(self):
        colliding = ["src/Component.tsx", "src/component.tsx"]
        with self.assertRaises(SecurityViolationError) as ctx:
            GitPolicy.validate_cross_platform_paths(colliding)
        self.assertIn("CASE_COLLISION_DETECTED", str(ctx.exception))

        non_colliding = ["src/Component.tsx", "src/OtherComponent.tsx"]
        GitPolicy.validate_cross_platform_paths(non_colliding)

    def test_gitattributes_safety_verification(self):
        safe_attributes = """
        # Line endings
        * text=auto eol=lf
        *.py text eol=lf
        *.js text eol=lf
        """
        self.assertTrue(GitPolicy.verify_gitattributes_safety(safe_attributes))

        unsafe_attributes = """
        # Missing eol=lf
        * text=auto
        """
        self.assertFalse(GitPolicy.verify_gitattributes_safety(unsafe_attributes))


if __name__ == "__main__":
    unittest.main()
