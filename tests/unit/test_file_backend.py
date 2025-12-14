"""Tests for file-based secret backend."""

import json
import pytest

from core.secrets.file_backend import FileSecretBackend
from core.secrets.exceptions import SecretBackendError, SecretNotFoundError


class TestFileSecretBackend:
    """Tests for FileSecretBackend."""

    def test_get_secret_success(self, tmp_path):
        """Should return value when key exists in file."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"DB_PASSWORD": "secret123"}))
        backend = FileSecretBackend(str(secrets_file))

        # Act
        result = backend.get_secret("DB_PASSWORD")

        # Assert
        assert result == "secret123"

    def test_get_secret_multiple_keys(self, tmp_path):
        """Should handle multiple secrets in file."""
        # Arrange
        secrets = {
            "DB_PASSWORD": "dbpass",
            "API_KEY": "apikey123",
            "KAFKA_HOST": "localhost:9092",
        }
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps(secrets))
        backend = FileSecretBackend(str(secrets_file))

        # Act & Assert
        assert backend.get_secret("DB_PASSWORD") == "dbpass"
        assert backend.get_secret("API_KEY") == "apikey123"
        assert backend.get_secret("KAFKA_HOST") == "localhost:9092"

    def test_get_secret_not_found(self, tmp_path):
        """Should raise SecretNotFoundError when key missing."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"OTHER_KEY": "value"}))
        backend = FileSecretBackend(str(secrets_file))

        # Act & Assert
        with pytest.raises(SecretNotFoundError) as exc_info:
            backend.get_secret("NONEXISTENT")

        assert "NONEXISTENT" in str(exc_info.value)

    def test_file_not_found(self):
        """Should raise SecretBackendError when file doesn't exist."""
        # Act & Assert
        with pytest.raises(SecretBackendError) as exc_info:
            FileSecretBackend("/nonexistent/path/secrets.json")

        assert "not found" in str(exc_info.value)

    def test_invalid_json(self, tmp_path):
        """Should raise SecretBackendError for invalid JSON."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text("not valid json {{{")

        # Act & Assert
        with pytest.raises(SecretBackendError) as exc_info:
            FileSecretBackend(str(secrets_file))

        assert "Invalid JSON" in str(exc_info.value)

    def test_health_check_file_exists(self, tmp_path):
        """Health check should return True when file exists."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"KEY": "value"}))
        backend = FileSecretBackend(str(secrets_file))

        # Act
        result = backend.health_check()

        # Assert
        assert result is True

    def test_converts_non_string_to_string(self, tmp_path):
        """Should convert non-string values to strings."""
        # Arrange
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"PORT": 5432, "ENABLED": True}))
        backend = FileSecretBackend(str(secrets_file))

        # Act & Assert
        assert backend.get_secret("PORT") == "5432"
        assert backend.get_secret("ENABLED") == "True"
