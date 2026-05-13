# src/lakehouse/utils/config_loader.py
"""
Reads the correct config file based on LAKEHOUSE_ENV.
Credentials are NEVER stored in config files — they are
always read from environment variables at runtime.

Priority order for credentials:
1. Environment variable (export MINIO_ACCESS_KEY=xxx)
2. .env file (loaded automatically by python-dotenv)
3. Default fallback (minioadmin — only works for local MinIO)
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load .env file automatically if it exists in project root.
# If .env doesn't exist, this does nothing — no error.
# Must be called before any os.getenv() calls.
load_dotenv()


def load_config() -> dict:
    """
    Loads configs/local.yml or configs/cloud.yml based on
    the LAKEHOUSE_ENV environment variable.

    LAKEHOUSE_ENV not set → defaults to "local"
    LAKEHOUSE_ENV=cloud   → loads configs/cloud.yml

    Returns:
        dict: full config with credentials injected from env vars

    Usage:
        from lakehouse.utils.config_loader import load_config
        config = load_config()
        total  = config["producer"]["total_events"]
        key    = config["storage"]["access_key"]  # from env var
    """
    env = os.getenv("LAKEHOUSE_ENV", "local")

    # Path(__file__) = this file:
    #   src/lakehouse/utils/config_loader.py
    # .parents[0] = src/lakehouse/utils/
    # .parents[1] = src/lakehouse/
    # .parents[2] = src/
    # .parents[3] = spark_project/  ← project root
    project_root = Path(__file__).parents[3]
    config_path  = project_root / "configs" / f"{env}.yml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"\nConfig file not found: {config_path}\n"
            f"LAKEHOUSE_ENV is set to '{env}'.\n"
            f"Make sure configs/{env}.yml exists.\n"
            f"Available envs: local, cloud"
        )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # ── Inject credentials from environment variables ──────────────
    # Credentials are NEVER read from the config file.
    # Config files are committed to git — credentials must not be.
    # Environment variables come from:
    #   - .env file (local development, never committed)
    #   - export commands in terminal (local switching)
    #   - GitHub Actions env: block (CI)
    #   - AWS IAM roles (cloud — no credentials needed at all)

    if "storage" in config:
        config["storage"]["access_key"] = os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin"   # fallback: only works for local Docker MinIO
        )
        config["storage"]["secret_key"] = os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin"   # fallback: only works for local Docker MinIO
        )

    # ── Inject Kafka credentials if needed ─────────────────────────
    # Local Docker Kafka needs no credentials.
    # Cloud MSK uses IAM authentication — no username/password.
    # This section is a placeholder for any future Kafka auth needs.
    if "kafka" in config:
        kafka_username = os.getenv("KAFKA_USERNAME")
        kafka_password = os.getenv("KAFKA_PASSWORD")
        if kafka_username:
            config["kafka"]["username"] = kafka_username
        if kafka_password:
            config["kafka"]["password"] = kafka_password

    print(f"[Config] Environment : {env}")
    print(f"[Config] Loaded from : {config_path}")
    print(f"[Config] Total events: {config.get('producer', {}).get('total_events', 'N/A'):,}")

    return config


def get_storage_path(config: dict, layer: str) -> str:
    """
    Returns the S3/MinIO path for a given layer.

    Args:
        config: loaded config dict from load_config()
        layer:  one of "bronze", "silver", "gold", "checkpoints"

    Returns:
        str: the full s3a:// path for that layer

    Usage:
        path = get_storage_path(config, "bronze")
        # local:  "s3a://bronze/events/"
        # cloud:  "s3://lakehouse-bronze-191367600538/events/"
    """
    valid_layers = ["bronze", "silver", "gold", "checkpoints"]

    if layer not in valid_layers:
        raise ValueError(
            f"Unknown layer: '{layer}'. "
            f"Must be one of: {valid_layers}"
        )

    return config["storage"][layer]


def get_checkpoint_path(config: dict, job_name: str) -> str:
    """
    Returns the checkpoint path for a specific streaming job.
    Each job gets its own subfolder to avoid checkpoint conflicts.

    Args:
        config:   loaded config dict
        job_name: name of the streaming job (e.g. "bronze_ingest")

    Returns:
        str: full checkpoint path for this job

    Usage:
        path = get_checkpoint_path(config, "bronze_ingest")
        # local: "s3a://checkpoints/bronze_ingest/"
        # cloud: "s3://lakehouse-checkpoints-191367600538/bronze_ingest/"
    """
    base = config["storage"]["checkpoints"]
    return f"{base}{job_name}/"