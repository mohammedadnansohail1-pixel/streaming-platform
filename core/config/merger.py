"""Deep merge logic for configuration files."""


def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dictionaries. Override wins on conflicts.

    Args:
        base: Base configuration
        override: Configuration to merge on top

    Returns:
        New merged dictionary

    Example:
        base = {"kafka": {"host": "localhost", "port": 9092}}
        override = {"kafka": {"port": 9093}}
        result = {"kafka": {"host": "localhost", "port": 9093}}
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Both are dicts - recurse
            result[key] = deep_merge(result[key], value)
        else:
            # Override wins
            result[key] = value

    return result
