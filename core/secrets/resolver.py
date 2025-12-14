"""Main secret resolver - resolves ${secret:KEY} patterns in config."""

import re
import logging
from typing import Any

from core.secrets.base import SecretBackend
from core.secrets.env_backend import EnvSecretBackend
from core.secrets.file_backend import FileSecretBackend
from core.secrets.exceptions import SecretBackendError

logger = logging.getLogger(__name__)

# Pattern to match ${secret:KEY_NAME}
SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")


class SecretResolver:
    """
    Resolves ${secret:KEY} patterns in configuration.

    Usage:
        resolver = SecretResolver(backend="env")

        # Resolve single value
        password = resolver.resolve_value("${secret:DB_PASSWORD}")

        # Resolve entire config dict
        config = resolver.resolve_config({
            "database": {
                "password": "${secret:DB_PASSWORD}"
            }
        })
    """

    def __init__(self, backend: str = "env", **backend_config):
        """
        Args:
            backend: Backend type ('env', 'file', 'vault', etc.)
            **backend_config: Backend-specific settings
        """
        self.backend = self._create_backend(backend, backend_config)
        self.backend_type = backend
        logger.info(f"Initialized SecretResolver with backend: {backend}")

    def _create_backend(self, backend: str, config: dict) -> SecretBackend:
        """
        Factory method to create appropriate backend.

        Args:
            backend: Backend type name
            config: Backend-specific configuration

        Returns:
            Configured SecretBackend instance

        Raises:
            SecretBackendError: If backend unknown or misconfigured
        """
        if backend == "env":
            return EnvSecretBackend(prefix=config.get("prefix", ""))

        elif backend == "file":
            if "path" not in config:
                raise SecretBackendError("File backend requires 'path' in config")
            return FileSecretBackend(file_path=config["path"])

        elif backend == "vault":
            raise NotImplementedError("Vault backend not yet implemented")

        elif backend == "aws_secrets":
            raise NotImplementedError("AWS Secrets Manager not yet implemented")

        elif backend == "azure_keyvault":
            raise NotImplementedError("Azure Key Vault not yet implemented")

        elif backend == "gcp_secrets":
            raise NotImplementedError("GCP Secret Manager not yet implemented")

        else:
            raise SecretBackendError(f"Unknown backend: {backend}")

    def resolve_value(self, value: str) -> str:
        """
        Resolve ${secret:KEY} patterns in a string.

        Args:
            value: String that may contain secret references

        Returns:
            String with secrets resolved

        Examples:
            "${secret:DB_PASSWORD}" -> "actual_password"
            "host:${secret:PORT}" -> "host:5432"
        """
        if not isinstance(value, str):
            return value

        def replace_match(match):
            key = match.group(1)
            return self.backend.get_secret(key)

        return SECRET_PATTERN.sub(replace_match, value)

    def resolve_config(self, config: Any) -> Any:
        """
        Recursively resolve all secrets in a config structure.

        Args:
            config: Dict, list, or value containing secret references

        Returns:
            Config with all ${secret:KEY} patterns resolved
        """
        if isinstance(config, dict):
            return {k: self.resolve_config(v) for k, v in config.items()}

        elif isinstance(config, list):
            return [self.resolve_config(item) for item in config]

        elif isinstance(config, str):
            return self.resolve_value(config)

        else:
            return config

    def health_check(self) -> bool:
        """Check if backend is healthy."""
        return self.backend.health_check()
