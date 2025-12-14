"""Configuration loader - loads and merges config files."""

import logging
from pathlib import Path
from typing import Optional

import yaml

from core.config.exceptions import ConfigNotFoundError, ConfigParseError
from core.config.merger import deep_merge
from core.secrets import SecretResolver

logger = logging.getLogger(__name__)

# Default paths relative to project root
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


class ConfigLoader:
    """
    Loads and merges configuration from multiple sources.

    Load order (later wins):
        1. platform.yaml (base infrastructure)
        2. environments/{env}.yaml (optional environment overrides)
        3. domains/{domain}.yaml (domain-specific config)
        4. Resolve ${secret:KEY} patterns

    Usage:
        loader = ConfigLoader()
        config = loader.load(domain="ecommerce", environment="prod")
    """

    def __init__(
        self,
        config_dir: Optional[Path] = None,
        secret_backend: str = "env",
        **secret_config,
    ):
        self.config_dir = Path(config_dir) if config_dir else CONFIG_DIR
        self.secret_resolver = SecretResolver(backend=secret_backend, **secret_config)

    def _load_yaml(self, path: Path) -> dict:
        """Load and parse a YAML file."""
        if not path.exists():
            raise ConfigNotFoundError(f"Config file not found: {path}")

        try:
            with open(path) as f:
                content = yaml.safe_load(f) or {}
                logger.debug(f"Loaded config: {path}")
                return content
        except yaml.YAMLError as e:
            raise ConfigParseError(f"Invalid YAML in {path}: {e}")

    def _load_if_exists(self, path: Path) -> dict:
        """Load YAML file if it exists, otherwise return empty dict."""
        if path.exists():
            return self._load_yaml(path)
        return {}

    def load(self, domain: str, environment: Optional[str] = None) -> dict:
        """
        Load complete configuration for a domain.

        Args:
            domain: Domain name (e.g., "ecommerce", "iot")
            environment: Optional environment (e.g., "prod", "staging")

        Returns:
            Merged and resolved configuration dict
        """
        # 1. Load base platform config
        platform_path = self.config_dir / "platform.yaml"
        config = self._load_yaml(platform_path)
        logger.info(f"Loaded platform config: {platform_path}")

        # 2. Merge environment overrides (if specified)
        if environment:
            env_path = self.config_dir / "environments" / f"{environment}.yaml"
            env_config = self._load_if_exists(env_path)
            if env_config:
                config = deep_merge(config, env_config)
                logger.info(f"Merged environment config: {env_path}")

        # 3. Merge domain config
        domain_path = self.config_dir / "domains" / f"{domain}.yaml"
        domain_config = self._load_yaml(domain_path)
        config = deep_merge(config, domain_config)
        logger.info(f"Merged domain config: {domain_path}")

        # 4. Resolve secrets
        config = self.secret_resolver.resolve_config(config)
        logger.info("Resolved secrets in config")

        return config

    def health_check(self) -> bool:
        """Check if config directory and secret backend are accessible."""
        return self.config_dir.exists() and self.secret_resolver.health_check()
