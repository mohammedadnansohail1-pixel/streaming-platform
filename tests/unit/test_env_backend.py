"""Tests for environment variable secret backend."""

import os
import pytest

from core.secrets.env_backend import EnvSecretBackend
from core.secrets.exceptions import SecretNotFoundError


class TestEnvSecretBackend:
    """Tests for EnvSecretBackend."""

    def test_get_secret_success(self):
        """Should return value when env var exists."""
        # Arrange
        os.environ["TEST_SECRET"] = "my_secret_value"
        backend = EnvSecretBackend()

        # Act
        result = backend.get_secret("TEST_SECRET")

        # Assert
        assert result == "my_secret_value"

        # Cleanup
        del os.environ["TEST_SECRET"]

    def test_get_secret_with_prefix(self):
        """Should prepend prefix to key."""
        # Arrange
        os.environ["APP_DB_PASSWORD"] = "secret123"
        backend = EnvSecretBackend(prefix="APP_")

        # Act
        result = backend.get_secret("DB_PASSWORD")

        # Assert
        assert result == "secret123"

        # Cleanup
        del os.environ["APP_DB_PASSWORD"]

    def test_get_secret_not_found(self):
        """Should raise SecretNotFoundError when env var missing."""
        # Arrange
        backend = EnvSecretBackend()

        # Make sure it doesn't exist
        if "NONEXISTENT_SECRET" in os.environ:
            del os.environ["NONEXISTENT_SECRET"]

        # Act & Assert
        with pytest.raises(SecretNotFoundError) as exc_info:
            backend.get_secret("NONEXISTENT_SECRET")

        assert "NONEXISTENT_SECRET" in str(exc_info.value)

    def test_health_check_always_true(self):
        """Health check should always return True for env backend."""
        # Arrange
        backend = EnvSecretBackend()

        # Act
        result = backend.health_check()

        # Assert
        assert result is True
