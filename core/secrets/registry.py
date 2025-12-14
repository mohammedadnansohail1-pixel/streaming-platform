"""Backend registry with decorator pattern."""

BACKENDS = {}


def register_backend(name: str):
    """
    Decorator to register a backend class.

    Usage:
        @register_backend("env")
        class EnvSecretBackend:
            ...
    """

    def decorator(cls):
        BACKENDS[name] = cls
        return cls

    return decorator


def get_backend(name: str):
    """
    Get backend class by name.

    Args:
        name: Backend identifier (env, file, vault, etc.)

    Returns:
        Backend class (not instance)

    Raises:
        KeyError: If backend not registered
    """
    if name not in BACKENDS:
        available = ", ".join(BACKENDS.keys()) or "none"
        raise KeyError(f"Unknown backend: '{name}'. Available: {available}")
    return BACKENDS[name]
