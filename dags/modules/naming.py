import re
from pathlib import Path


def clean_id(value: str) -> str:
    """Basic cleaning for file/ID parts."""
    if not value:
        return ""
    # Strict cleaning: replace spaces, hyphens, and other non-alphanumerics with underscore
    return re.sub(r"[^a-zA-Z0-9_]", "_", value.strip()).lower()


def get_pipeline_prefix(config: dict) -> str:
    """Determines the standard prefix for a pipeline based on its config."""
    is_spark     = bool(config.get("spark"))
    pipeline_type = config.get("pipeline_type")

    if pipeline_type == "hybrid":
        # An API job is a standalone job type from the user's perspective
        # (own wizard, own routes) but is implemented under the hood as
        # pipeline_type "hybrid" with source_type "api", to reuse hybrid's
        # raw -> refined -> target code. It still gets its own "api_" dag_id
        # prefix, distinct from a real file/db hybrid job.
        return "api" if config.get("source_type") == "api" else "hybrid"
    elif pipeline_type == "file_based":
        return "spark_file" if is_spark else "file"
    else:  # DB-to-DB
        return "spark_db" if is_spark else "db"


def generate_dag_id(config: dict, bare_name: str) -> str:
    """
    Generates a standardized DAG ID from a config object and a user-provided bare name.
    Example:
    - config(file, spark), bare_name="wine" -> "spark_file_myconn_wine"
    - config(db, no-spark), bare_name="invoices" -> "db_myconn_invoices"
    """
    prefix = get_pipeline_prefix(config)
    target_cfg = config.get("target", {}).get("dev", {})
    conn_id = clean_id(target_cfg.get("target_db_conn_id", "default"))

    # Clean the user input of any manually typed prefixes to avoid duplication.
    # This handles both fresh names and full dag_ids pasted back from the edit form.
    # Pattern: {prefix}_{conn_id}_{bare_name} — strip prefix and conn_id if present.
    clean_bare_name = bare_name
    if prefix == "hybrid":
        clean_bare_name = re.sub(r'^hybrid_', '', clean_bare_name, flags=re.IGNORECASE)
    elif prefix == "api":
        clean_bare_name = re.sub(r'^api_', '', clean_bare_name, flags=re.IGNORECASE)
    elif prefix in ("spark_file", "file"):
        clean_bare_name = re.sub(r'^(spark_file_|file_)(csv|parquet|)', '', clean_bare_name, flags=re.IGNORECASE)
        clean_bare_name = re.sub(r'^(Spark_File_|File_)(csv|parquet)_', '', clean_bare_name, flags=re.IGNORECASE)
    elif prefix in ("spark_db", "db"):
        clean_bare_name = re.sub(r'^(spark_db_|db_)', '', clean_bare_name, flags=re.IGNORECASE)
        clean_bare_name = re.sub(r'^(Spark_DB_|DB_)', '', clean_bare_name, flags=re.IGNORECASE)

    # After stripping the type prefix, also strip the conn_id portion if it was
    # already embedded (happens when the full dag_id is re-submitted on edit).
    if conn_id and clean_bare_name.lower().startswith(conn_id.lower() + "_"):
        clean_bare_name = clean_bare_name[len(conn_id) + 1:]

    clean_bare_name = clean_id(clean_bare_name)
    return f"{prefix}_{conn_id}_{clean_bare_name}"


def get_config_path(dag_id: str, config: dict, config_dir: Path) -> Path:
    """Gets the standardized path for a JSON config file."""
    target_cfg = config.get("target", {}).get("dev", {})
    conn_id = clean_id(target_cfg.get("target_db_conn_id", "default"))
    filename = f"{dag_id}.json"
    return config_dir / conn_id / filename


def get_dag_py_path(dag_id: str, config: dict, etl_dir: Path) -> Path:
    """Gets the standardized path for a generated DAG .py file."""
    target_cfg = config.get("target", {}).get("dev", {})
    conn_id = clean_id(target_cfg.get("target_db_conn_id", "default"))
    filename = f"{dag_id}.py"
    return etl_dir / conn_id / filename


def find_config_path(dag_id: str, config_dir: Path) -> Path:
    """
    Finds a config file by its dag_id, searching nested connection subdirectories.
    """
    # 1. Exact match (modern standard)
    for path in config_dir.rglob(f"{dag_id}.json"):
        if path.is_file():
            return path
            
    # 2. Fallback: looser search if exact match fails (for legacy migration)
    # matches = list(config_dir.rglob(f"*{dag_id}*.json"))
    # return matches[0] if matches else None
    return None