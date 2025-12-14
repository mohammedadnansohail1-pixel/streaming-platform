"""File-based secret backend."""

import json
import logging
from pathlib import Path

from core.secrets.base import SecretBackend
from core.secrets.exceptions import SecretBackendError, SecretNotFoundError

logger = logging.getLogger(__name__)


class FileSecretBackend(SecretBackend):
    """
    Reads secrets from a JSON file.

    File format:
        {
            "DB_PASSWORD": "secret123",
            "KAFKA_HOST": "localhost:9092"
        }
    """

    def __init__(self, file_path: str):
        """
        Args:
            file_path: Path to JSON secrets file

        Raises:
            SecretBackendError: If file not found or invalid JSON
        """
        self.file_path = Path(file_path)
        self._secrets: dict = {}
        self._load_secrets()

    def _load_secrets(self) -> None:
        """Load secrets from file."""
        if not self.file_path.exists():
            raise SecretBackendError(f"Secret file not found: {self.file_path}")

        try:
            with open(self.file_path) as f:
                self._secrets = json.load(f)
        except json.JSONDecodeError as e:
            raise SecretBackendError(f"Invalid JSON in {self.file_path}: {e}")

        logger.info(f"Loaded {len(self._secrets)} secrets from {self.file_path}")

    def get_secret(self, key: str) -> str:
        """
        Get secret from loaded file.

        Args:
            key: Secret key (exact match)

        Returns:
            Secret value

        Raises:
            SecretNotFoundError: If key not in file
        """
        if key not in self._secrets:
            raise SecretNotFoundError(f"Secret '{key}' not found in {self.file_path}")

        return str(self._secrets[key])

    def health_check(self) -> bool:
        """Check if file exists and is readable."""
        return self.file_path.exists()
