"""Main secret resolver - resolves ${secret:KEY} patterns in config."""

import re
import logging
from typing import Any

# Import backends to trigger registration
# Import backends to trigger registration
from core.secrets import env_backend, file_backend  # noqa: F401
from core.secrets.registry import get_backend
from core.secrets.exceptions import SecretBackendError

logger = logging.getLogger(__name__)

SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")


class SecretResolver:
    """
    Resolves ${secret:KEY} patterns in configuration.

    Usage:
        resolver = SecretResolver(backend="env")
        config = resolver.resolve_config({"password": "${secret:DB_PASS}"})
    """

    def __init__(self, backend: str = "env", **backend_config):
        self.backend_type = backend
        self.backend = self._create_backend(backend, backend_config)
        logger.info(f"Initialized SecretResolver with backend: {backend}")

    def _create_backend(self, backend: str, config: dict):
        """Create backend using registry."""
        try:
            backend_cls = get_backend(backend)
            return backend_cls(**config)
        except KeyError as e:
            raise SecretBackendError(str(e))
        except TypeError as e:
            # Missing required argument (e.g., file backend needs path)
            raise SecretBackendError(f"Backend '{backend}' config error: {e}")

    def resolve_value(self, value: str) -> str:
        """Resolve ${secret:KEY} patterns in a string."""
        if not isinstance(value, str):
            return value

        def replace_match(match):
            key = match.group(1)
            return self.backend.get_secret(key)

        return SECRET_PATTERN.sub(replace_match, value)

    def resolve_config(self, config: Any) -> Any:
        """Recursively resolve all secrets in a config structure."""
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
