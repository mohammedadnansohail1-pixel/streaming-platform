"""File-based secret backend."""

import json
from pathlib import Path

from core.secrets.base import SecretBackend
from core.secrets.registry import register_backend
from core.secrets.exceptions import SecretNotFoundError, SecretBackendError


@register_backend("file")
class FileSecretBackend(SecretBackend):
    """
    Retrieves secrets from a JSON file.

    Args:
        path: Path to JSON file containing secrets
    """

    def __init__(self, path: str):
        self.file_path = Path(path)
        self._secrets = {}
        self._load_secrets()

    def _load_secrets(self):
        """Load secrets from file."""
        if not self.file_path.exists():
            raise SecretBackendError(f"Secrets file not found: {self.file_path}")

        try:
            with open(self.file_path) as f:
                self._secrets = json.load(f)
        except json.JSONDecodeError as e:
            raise SecretBackendError(f"Invalid JSON in {self.file_path}: {e}")

    def get_secret(self, key: str) -> str:
        """Get secret from loaded file."""
        if key not in self._secrets:
            raise SecretNotFoundError(f"Secret '{key}' not found in {self.file_path}")

        return str(self._secrets[key])

    def health_check(self) -> bool:
        """Check if file is readable."""
        return self.file_path.exists()
