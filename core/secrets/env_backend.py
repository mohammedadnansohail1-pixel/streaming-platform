"""Environment variable secret backend."""

import os

from core.secrets.base import SecretBackend
from core.secrets.registry import register_backend
from core.secrets.exceptions import SecretNotFoundError


@register_backend("env")
class EnvSecretBackend(SecretBackend):
    """
    Retrieves secrets from environment variables.

    Args:
        prefix: Optional prefix for env var names
                e.g., prefix="APP_" means get_secret("DB_PASS") looks up "APP_DB_PASS"
    """

    def __init__(self, prefix: str = ""):
        self.prefix = prefix

    def get_secret(self, key: str) -> str:
        """Get secret from environment variable."""
        env_key = f"{self.prefix}{key}"
        value = os.environ.get(env_key)

        if value is None:
            raise SecretNotFoundError(f"Environment variable not found: {env_key}")

        return value

    def health_check(self) -> bool:
        """Env vars are always accessible."""
        return True
