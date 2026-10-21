"""Git branch policies, cross-platform normalization, and defense-in-depth safeguards."""
import os
from pathlib import Path
import re
from typing import List, Optional

from orchestration.errors import SecurityViolationError

PROTECTED_BRANCHES = {"main", "master", "production", "prod", "stable"}
PROTECTED_PREFIXES = ("release/", "releases/", "hotfix/", "infra/")
VALID_BRANCH_RE = re.compile(r"^[a-zA-Z0-9_\-\.\/]+$")
AGENT_BRANCH_PATTERN = re.compile(r"^(duality|agent)\/[a-zA-Z0-9_\-]+\/[a-zA-Z0-9_\-]+$")

# Windows reserved file names that corrupt NTFS checkouts
WINDOWS_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
}


class GitPolicy:
    """Enforces defense-in-depth policies for multi-agent Git interactions.
    
    NOTE: These checks serve as an internal application-layer safeguard.
    They do NOT substitute for true server-side branch protections and
    access control lists (e.g., GitHub Branch Protection Rules / push policies).
    """

    @classmethod
    def validate_target_branch(cls, branch: str, allow_protected: bool = False) -> str:
        """Ensure no agent directly targets protected branches like 'main'."""
        if not isinstance(branch, str) or not branch.strip():
            raise SecurityViolationError("INVALID_BRANCH_NAME", "Branch name cannot be empty")

        clean_branch = branch.strip()

        if not VALID_BRANCH_RE.fullmatch(clean_branch) or ".." in clean_branch or clean_branch.startswith("/"):
            raise SecurityViolationError("INVALID_BRANCH_NAME", f"Malformed branch name: '{clean_branch}'")

        if not allow_protected:
            # Reject main, master, prod, etc.
            if clean_branch.lower() in PROTECTED_BRANCHES:
                raise SecurityViolationError(
                    "PROTECTED_BRANCH_MODIFICATION_BLOCKED",
                    f"Direct target branch '{clean_branch}' is strictly protected. Only human-approved merges are permitted."
                )
            for prefix in PROTECTED_PREFIXES:
                if clean_branch.lower().startswith(prefix):
                    raise SecurityViolationError(
                        "PROTECTED_BRANCH_MODIFICATION_BLOCKED",
                        f"Target branch '{clean_branch}' starts with protected prefix '{prefix}'."
                    )

        return clean_branch

    @classmethod
    def validate_agent_branch(cls, branch: str) -> str:
        """Validate that agent work branches adhere to isolated naming patterns."""
        clean_branch = cls.validate_target_branch(branch, allow_protected=False)
        if not AGENT_BRANCH_PATTERN.fullmatch(clean_branch):
            raise SecurityViolationError(
                "AGENT_BRANCH_CONVENTION_VIOLATION",
                f"Agent branch '{clean_branch}' must follow format 'duality/<task_id>/<agent_id>' or 'agent/<agent_id>/<task_id>'"
            )
        return clean_branch

    @classmethod
    def validate_cross_platform_paths(cls, file_paths: List[str]) -> None:
        """Validate paths against Windows/Linux cross-platform incompatibilities."""
        seen_case_insensitive = {}

        for raw_path in file_paths:
            norm_path = raw_path.replace('\\', '/').strip('/')
            parts = norm_path.split('/')

            for part in parts:
                base_name = part.split('.')[0].upper()
                if base_name in WINDOWS_RESERVED_NAMES:
                    raise SecurityViolationError(
                        "WINDOWS_RESERVED_FILENAME",
                        f"Path component '{part}' in '{raw_path}' is a reserved device name on Windows"
                    )

            lower_path = norm_path.lower()
            if lower_path in seen_case_insensitive:
                existing = seen_case_insensitive[lower_path]
                if existing != norm_path:
                    raise SecurityViolationError(
                        "CASE_COLLISION_DETECTED",
                        f"Paths '{existing}' and '{norm_path}' collide on case-insensitive filesystems (Windows)"
                    )
            seen_case_insensitive[lower_path] = norm_path

    @classmethod
    def verify_gitattributes_safety(cls, gitattributes_content: str) -> bool:
        """Verify that .gitattributes enforces deterministic LF line endings across platforms."""
        lines = [line.strip() for line in gitattributes_content.splitlines() if line.strip() and not line.strip().startswith('#')]
        has_eol_lf = any("eol=lf" in line for line in lines)
        has_text_auto = any("text=auto" in line or "* text" in line for line in lines)
        return has_eol_lf and has_text_auto
