"""Path validation, workspace confinement, and cross-platform filesystem security."""
import os
from pathlib import Path
import stat
import sys
from typing import List, Optional

from orchestration.errors import PathTraversalError, SecurityViolationError

# Reparse point flag on Windows NTFS (covers symlinks, junctions, and volume mounts)
FILE_ATTRIBUTE_REPARSE_POINT = 0x0400


class PathSecurity:
    """Enforces strict path containment, rejects symlinks, junctions, and directory escapes."""

    @staticmethod
    def is_reparse_point_or_symlink(path: Path) -> bool:
        """Check if a path component is a symlink or an NTFS Junction / reparse point."""
        if not path.exists() and not path.is_symlink():
            return False
        
        # Standard Python check (detects symlinks and junctions on modern Python)
        try:
            if path.is_symlink():
                return True
        except OSError:
            return True

        # Low-level Windows attribute check for reparse points (NTFS junctions)
        if sys.platform == 'win32':
            try:
                st = os.lstat(str(path))
                attrs = getattr(st, 'st_file_attributes', 0)
                if attrs & FILE_ATTRIBUTE_REPARSE_POINT:
                    return True
            except OSError:
                pass

        # Low-level POSIX check
        if os.name == 'posix':
            try:
                st = os.lstat(str(path))
                if stat.S_ISLNK(st.st_mode):
                    return True
            except OSError:
                pass

        return False

    @staticmethod
    def validate_component(name: str) -> str:
        """Validate a single path component against traversal and injection."""
        if not isinstance(name, str) or not name:
            raise SecurityViolationError("INVALID_PATH_COMPONENT", "Component must be a non-empty string")
        if name in ('.', '..'):
            raise SecurityViolationError("INVALID_PATH_COMPONENT", "Directory traversal components forbidden")
        if '/' in name or '\\' in name or '\0' in name:
            raise SecurityViolationError("INVALID_PATH_COMPONENT", "Path separators and null bytes forbidden in component")
        return name

    @classmethod
    def resolve_confined_path(cls, workspace_root: str, relative_path: str, allow_symlinks: bool = False) -> Path:
        """Resolve a relative path ensuring it strictly resides within workspace_root.
        
        Rejects directory traversal, null bytes, absolute paths, NTFS junctions,
        and verifies step-by-step that no component escapes the root.
        """
        if '\0' in workspace_root or '\0' in relative_path:
            raise SecurityViolationError("NULL_BYTE_IN_PATH", "Path contains null bytes")

        # Reject absolute paths immediately (POSIX leading slash, Windows backslash, or drive letter C:)
        if (relative_path.startswith('/') or relative_path.startswith('\\')
                or (len(relative_path) >= 2 and relative_path[1] == ':')):
            raise SecurityViolationError(
                "ABSOLUTE_PATH_FORBIDDEN",
                f"Path '{relative_path}' is absolute; only relative paths inside workspace permitted"
            )

        root_path = Path(workspace_root).resolve()
        if not root_path.is_dir():
            raise SecurityViolationError("WORKSPACE_NOT_DIRECTORY", f"Workspace root '{workspace_root}' is not a directory")

        # Root itself must not be a symlink or junction
        if not allow_symlinks and cls.is_reparse_point_or_symlink(root_path):
            raise SecurityViolationError("WORKSPACE_ROOT_IS_SYMLINK", f"Workspace root '{root_path}' is a symlink or junction")

        # Normalize relative path
        rel = relative_path.replace('\\', '/').strip('/')
        if not rel:
            return root_path

        parts = [p for p in rel.split('/') if p]
        for part in parts:
            if part in ('.', '..'):
                raise PathTraversalError(relative_path, str(root_path))

        # Incremental resolution and component-level reparse/symlink check
        current = root_path
        for part in parts:
            current = current / part
            if not allow_symlinks and cls.is_reparse_point_or_symlink(current):
                raise SecurityViolationError(
                    "SYMLINK_OR_JUNCTION_FORBIDDEN",
                    f"Symlink or junction detected at component '{current}'"
                )

        target_path = current.resolve()

        # Strict containment verification
        try:
            target_path.relative_to(root_path)
        except ValueError:
            raise PathTraversalError(relative_path, str(root_path))

        return target_path

    @staticmethod
    def check_case_collision(existing_paths: List[str], candidate_path: str) -> Optional[str]:
        """Detect case collisions that would cause issues on case-insensitive filesystems (Windows/macOS)."""
        candidate_norm = candidate_path.lower().replace('\\', '/')
        for existing in existing_paths:
            if existing.lower().replace('\\', '/') == candidate_norm and existing != candidate_path:
                return existing
        return None
