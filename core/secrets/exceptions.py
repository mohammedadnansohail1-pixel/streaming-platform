"""Custom exceptions for secret management."""


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in the backend."""

    pass


class SecretBackendError(Exception):
    """Raised when there's an issue with the secret backend itself."""

    pass
