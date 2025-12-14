"""Secrets management module."""

# Import backends to trigger registration

# Public API
from core.secrets.resolver import SecretResolver
from core.secrets.exceptions import SecretNotFoundError, SecretBackendError
from core.secrets.registry import register_backend, get_backend

__all__ = [
    "SecretResolver",
    "SecretNotFoundError",
    "SecretBackendError",
    "register_backend",
    "get_backend",
]
