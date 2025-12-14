"""Tests for secret resolver."""

import os
import json
import pytest

from core.secrets.resolver import SecretResolver
from core.secrets.exceptions import SecretBackendError, SecretNotFoundError


class TestSecretResolverEnvBackend:
    """Tests for SecretResolver with env backend."""

    def test_resolve_value_simple(self):
        """Should resolve single secret reference."""
        # Arrange
        os.environ["DB_PASSWORD"] = "secret123"
        resolver = SecretResolver(backend="env")

        # Act
        result = resolver.resolve_value("${secret:DB_PASSWORD}")

        # Assert
        assert result == "secret123"

        # Cleanup
        del os.environ["DB_PASSWORD"]

    def test_resolve_value_with_surrounding_text(self):
        """Should resolve secret within other text."""
        # Arrange
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = "5432"
        resolver = SecretResolver(backend="env")

        # Act
        result = resolver.resolve_value(
            "jdbc:postgresql://${secret:DB_HOST}:${secret:DB_PORT}/db"
        )

        # Assert
        assert result == "jdbc:postgresql://localhost:5432/db"

        # Cleanup
        del os.environ["DB_HOST"]
        del os.environ["DB_PORT"]

    def test_resolve_value_no_secret(self):
        """Should return original string if no secret pattern."""
        # Arrange
        resolver = SecretResolver(backend="env")

        # Act
        result = resolver.resolve_value("plain text no secrets")

        # Assert
        assert result == "plain text no secrets"

    def test_resolve_config_nested(self):
        """Should resolve secrets in nested dict."""
        # Arrange
        os.environ["DB_PASSWORD"] = "secret123"
        os.environ["API_KEY"] = "api456"
        resolver = SecretResolver(backend="env")

        config = {
            "database": {"host": "localhost", "password": "${secret:DB_PASSWORD}"},
            "api": {"key": "${secret:API_KEY}"},
        }

        # Act
        result = resolver.resolve_config(config)

        # Assert
        assert result["database"]["host"] == "localhost"
        assert result["database"]["password"] == "secret123"
        assert result["api"]["key"] == "api456"

        # Cleanup
        del os.environ["DB_PASSWORD"]
        del os.environ["API_KEY"]

    def test_resolve_config_with_list(self):
        """Should resolve secrets inside lists."""
        # Arrange
        os.environ["HOST1"] = "server1"
        os.environ["HOST2"] = "server2"
        resolver = SecretResolver(backend="env")

        config = {"hosts": ["${secret:HOST1}", "${secret:HOST2}"]}

        # Act
        result = resolver.resolve_config(config)

        # Assert
        assert result["hosts"] == ["server1", "server2"]

        # Cleanup
        del os.environ["HOST1"]
        del os.environ["HOST2"]

    def test_resolve_config_preserves_non_strings(self):
        """Should preserve integers, booleans, etc."""
        # Arrange
        resolver = SecretResolver(backend="env")

        config = {"port": 5432, "enabled": True, "ratio": 0.5, "nothing": None}

        # Act
        result = resolver.resolve_config(config)

        # Assert
        assert result["port"] == 5432
        assert result["enabled"] is True
        assert result["ratio"] == 0.5
        assert result["nothing"] is None

    def test_secret_not_found_raises_error(self):
        """Should raise error when secret not found."""
        # Arrange
        resolver = SecretResolver(backend="env")

        # Act & Assert
        with pytest.raises(SecretNotFoundError):
            resolver.resolve_value("${secret:NONEXISTENT_SECRET}")

    def test_health_check(self):
        """Health check should return True for env backend."""
        # Arrange
        resolver = SecretResolver(backend="env")

        # Act & Assert
        assert resolver.health_check() is True


class TestSecretResolverFileBackend:
    """Tests for SecretResolver with file backend."""

    def test_resolve_with_file_backend(self, tmp_path):
        """Should resolve secrets from file."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"DB_PASSWORD": "filepass"}))
        resolver = SecretResolver(backend="file", path=str(secrets_file))

        # Act
        result = resolver.resolve_value("${secret:DB_PASSWORD}")

        # Assert
        assert result == "filepass"

    def test_file_backend_requires_path(self):
        """Should raise error when path not provided."""
        # Act & Assert
        with pytest.raises(SecretBackendError) as exc_info:
            SecretResolver(backend="file")

        assert "path" in str(exc_info.value)


class TestSecretResolverUnknownBackend:
    """Tests for unknown backend handling."""

    def test_unknown_backend_raises_error(self):
        """Should raise error for unknown backend."""
        # Act & Assert
        with pytest.raises(SecretBackendError) as exc_info:
            SecretResolver(backend="unknown_backend")

        assert "Unknown backend" in str(exc_info.value)

    def test_vault_not_implemented(self):
        """Vault should raise NotImplementedError."""
        # Act & Assert
        with pytest.raises(NotImplementedError):
            SecretResolver(backend="vault")
