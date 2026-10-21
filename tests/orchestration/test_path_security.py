"""Unit tests for path traversal, workspace confinement, and symlink/junction checks."""
import os
from pathlib import Path
import tempfile
import unittest

from orchestration.errors import PathTraversalError, SecurityViolationError
from orchestration.path_security import PathSecurity


class TestPathSecurity(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name).resolve()
        (self.workspace / "sub").mkdir(parents=True, exist_ok=True)
        (self.workspace / "sub" / "file.txt").write_text("content", encoding="utf-8")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_valid_relative_path_resolution(self):
        resolved = PathSecurity.resolve_confined_path(str(self.workspace), "sub/file.txt")
        self.assertEqual(resolved, (self.workspace / "sub" / "file.txt").resolve())

    def test_directory_traversal_attempts_blocked(self):
        traversal_attempts = [
            "../outside.txt",
            "sub/../../outside.txt",
            "..\\outside.txt",
            "/etc/passwd",
            "\\Windows\\System32",
            "C:\\Windows\\System32",
            "sub/../../../etc/shadow",
        ]
        for attempt in traversal_attempts:
            with self.subTest(attempt=attempt):
                with self.assertRaises(SecurityViolationError):
                    PathSecurity.resolve_confined_path(str(self.workspace), attempt)

    def test_null_bytes_in_paths_blocked(self):
        with self.assertRaises(SecurityViolationError):
            PathSecurity.resolve_confined_path(str(self.workspace), "sub/file.txt\0.png")

    def test_symlink_rejection(self):
        target = self.workspace / "sub" / "file.txt"
        symlink = self.workspace / "sub" / "link_to_file.txt"
        try:
            os.symlink(str(target), str(symlink))
        except (OSError, NotImplementedError):
            # In Windows, creating symlinks may require elevated privileges unless Developer Mode is on
            self.skipTest("Symlink creation not permitted in current OS environment")

        with self.assertRaises(SecurityViolationError) as ctx:
            PathSecurity.resolve_confined_path(str(self.workspace), "sub/link_to_file.txt")
        self.assertIn("SYMLINK_OR_JUNCTION_FORBIDDEN", str(ctx.exception))

    def test_component_validation(self):
        self.assertEqual(PathSecurity.validate_component("valid_name-1.py"), "valid_name-1.py")

        for bad in [".", "..", "foo/bar", "foo\\bar", "foo\0bar", ""]:
            with self.subTest(bad=bad):
                with self.assertRaises(SecurityViolationError):
                    PathSecurity.validate_component(bad)

    def test_case_collision_detection(self):
        existing = ["src/module.py", "src/auth/User.py"]
        
        collision = PathSecurity.check_case_collision(existing, "src/MODULE.py")
        self.assertEqual(collision, "src/module.py")

        collision2 = PathSecurity.check_case_collision(existing, "src/auth/user.py")
        self.assertEqual(collision2, "src/auth/User.py")

        self.assertIsNone(PathSecurity.check_case_collision(existing, "src/other.py"))


if __name__ == "__main__":
    unittest.main()
