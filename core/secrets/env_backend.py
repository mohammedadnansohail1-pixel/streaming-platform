"""Environment variable secret backend."""

import os
import logging

from core.secrets.base import SecretBackend
from core.secrets.exceptions import SecretNotFoundError

logger = logging.getLogger(__name__)


class EnvSecretBackend(SecretBackend):
    """
    Reads secrets from environment variables.

    Uses exact key match - no conversion.

    Examples:
        ${secret:DB_PASSWORD} -> env var DB_PASSWORD
        ${secret:KAFKA_BOOTSTRAP_SERVERS} -> env var KAFKA_BOOTSTRAP_SERVERS
    """

    def __init__(self, prefix: str = ""):
        """
        Args:
            prefix: Optional prefix for env vars (e.g., 'APP_' -> APP_DB_PASSWORD)
        """
        self.prefix = prefix

    def get_secret(self, key: str) -> str:
        """
        Get secret from environment variable.

        Args:
            key: Secret key (exact match to env var name)

        Returns:
            Secret value

        Raises:
            SecretNotFoundError: If env var not set
        """
        env_key = f"{self.prefix}{key}"
        value = os.environ.get(env_key)

        if value is None:
            raise SecretNotFoundError(
                f"Secret '{key}' not found. Set environment variable: {env_key}"
            )

        logger.debug(f"Resolved secret '{key}' from env var '{env_key}'")
        return value

    def health_check(self) -> bool:
        """Environment backend is always available."""
        return True
