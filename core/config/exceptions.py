"""Configuration-related exceptions."""


class ConfigError(Exception):
    """Base exception for config errors."""

    pass


class ConfigNotFoundError(ConfigError):
    """Raised when config file doesn't exist."""

    pass


class ConfigParseError(ConfigError):
    """Raised when config file has invalid YAML."""

    pass


class ConfigValidationError(ConfigError):
    """Raised when config fails validation."""

    pass
