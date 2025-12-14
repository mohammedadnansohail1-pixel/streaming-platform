"""Tests for configuration loader."""

import pytest
import os

from core.config.loader import ConfigLoader
from core.config.exceptions import ConfigNotFoundError, ConfigParseError


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_platform_and_domain(self, tmp_path):
        """Should load and merge platform + domain config."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text(
            """
kafka:
  bootstrap_servers: localhost:9092
  batch_size: 1000
"""
        )
        (config_dir / "domains" / "ecommerce.yaml").write_text(
            """
domain:
  name: ecommerce
  entity: user_id
"""
        )

        loader = ConfigLoader(config_dir=config_dir)

        # Act
        config = loader.load(domain="ecommerce")

        # Assert
        assert config["kafka"]["bootstrap_servers"] == "localhost:9092"
        assert config["domain"]["name"] == "ecommerce"

    def test_environment_overrides_platform(self, tmp_path):
        """Environment config should override platform defaults."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "environments").mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text(
            """
kafka:
  batch_size: 1000
  retries: 3
"""
        )
        (config_dir / "environments" / "prod.yaml").write_text(
            """
kafka:
  batch_size: 5000
"""
        )
        (config_dir / "domains" / "ecommerce.yaml").write_text(
            """
domain:
  name: ecommerce
"""
        )

        loader = ConfigLoader(config_dir=config_dir)

        # Act
        config = loader.load(domain="ecommerce", environment="prod")

        # Assert
        assert config["kafka"]["batch_size"] == 5000  # overridden
        assert config["kafka"]["retries"] == 3  # preserved from platform

    def test_domain_overrides_all(self, tmp_path):
        """Domain config should override platform and environment."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "environments").mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text(
            """
processing:
  window_size: 60
"""
        )
        (config_dir / "environments" / "prod.yaml").write_text(
            """
processing:
  window_size: 120
"""
        )
        (config_dir / "domains" / "iot.yaml").write_text(
            """
processing:
  window_size: 30
"""
        )

        loader = ConfigLoader(config_dir=config_dir)

        # Act
        config = loader.load(domain="iot", environment="prod")

        # Assert
        assert config["processing"]["window_size"] == 30  # domain wins

    def test_missing_environment_is_ok(self, tmp_path):
        """Should continue if environment file doesn't exist."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text("kafka: {}")
        (config_dir / "domains" / "ecommerce.yaml").write_text("domain: {}")

        loader = ConfigLoader(config_dir=config_dir)

        # Act - no environments folder, no error
        config = loader.load(domain="ecommerce", environment="prod")

        # Assert
        assert "kafka" in config

    def test_missing_platform_raises_error(self, tmp_path):
        """Should raise error if platform.yaml missing."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        loader = ConfigLoader(config_dir=config_dir)

        # Act & Assert
        with pytest.raises(ConfigNotFoundError) as exc_info:
            loader.load(domain="ecommerce")

        assert "platform.yaml" in str(exc_info.value)

    def test_missing_domain_raises_error(self, tmp_path):
        """Should raise error if domain config missing."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text("kafka: {}")

        loader = ConfigLoader(config_dir=config_dir)

        # Act & Assert
        with pytest.raises(ConfigNotFoundError) as exc_info:
            loader.load(domain="nonexistent")

        assert "nonexistent.yaml" in str(exc_info.value)

    def test_invalid_yaml_raises_error(self, tmp_path):
        """Should raise error on invalid YAML."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        (config_dir / "platform.yaml").write_text("invalid: yaml: content: {{")

        loader = ConfigLoader(config_dir=config_dir)

        # Act & Assert
        with pytest.raises(ConfigParseError):
            loader.load(domain="ecommerce")

    def test_resolves_secrets(self, tmp_path):
        """Should resolve ${secret:KEY} patterns."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "domains").mkdir()

        (config_dir / "platform.yaml").write_text(
            """
database:
  password: ${secret:DB_PASSWORD}
"""
        )
        (config_dir / "domains" / "ecommerce.yaml").write_text("domain: {}")

        os.environ["DB_PASSWORD"] = "supersecret"
        loader = ConfigLoader(config_dir=config_dir)

        # Act
        config = loader.load(domain="ecommerce")

        # Assert
        assert config["database"]["password"] == "supersecret"

        # Cleanup
        del os.environ["DB_PASSWORD"]

    def test_health_check(self, tmp_path):
        """Health check should verify config dir exists."""
        # Arrange
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        loader = ConfigLoader(config_dir=config_dir)

        # Assert
        assert loader.health_check() is True

    def test_health_check_fails_for_missing_dir(self, tmp_path):
        """Health check should fail if config dir missing."""
        # Arrange
        loader = ConfigLoader(config_dir=tmp_path / "nonexistent")

        # Assert
        assert loader.health_check() is False
