"""
Symmetric encryption for secrets stored in ETL Manager pipeline configs
(e.g. API job auth: static API keys, bearer tokens, login passwords).

This module is intentionally dependency-light (only `cryptography`, already
a declared dependency of both the web UI and the Airflow image) and imports
nothing Flask- or Airflow-specific, so it loads identically whether it's
imported from `web_ui/app.py` (the wizard, encrypting on save / decrypting
for "Test Connection") or from a generated DAG under `dags/etl/**/*.py` at
Airflow-worker runtime (decrypting to make the live API call) — mirroring
how `dags/modules/naming.py` is already dual-imported from both processes.

Key sourcing follows the same convention as every other secret in this
project (see `web_ui/app.py`'s `_secret()` helper and `scripts/init-secrets.sh`):
read `/run/secrets/webui_fernet_key` (a Docker secret) first, fall back to
the `WEBUI_FERNET_KEY` env var. This is a NEW, dedicated key — distinct from
Airflow's own `AIRFLOW__CORE__FERNET_KEY` (which only ever encrypts Airflow's
own `connection` table, used read-only elsewhere in this codebase) and from
the Flask `SECRET_KEY` (session-cookie signing, unrelated purpose). Never
reuse either of those for this.

Generate a key once with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
and provision it the same way as every other secret (see scripts/init-secrets.sh
and docker-compose.yaml's `x-secrets` block). Rotating this key invalidates
every previously-encrypted secret in every saved API job config — there is
no key-rotation / re-encryption path in this module.
"""

import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

_SECRET_NAME = "webui_fernet_key"
_ENV_VAR = "WEBUI_FERNET_KEY"

# Sentinel the wizard pre-fills a secret field with instead of ever rendering
# a decrypted value back into HTML. If a form submits this value unchanged,
# the caller should keep the existing ciphertext rather than re-encrypting it.
MASK = "•" * 8  # "••••••••"


class SecretsNotConfigured(RuntimeError):
    """Raised when no Fernet key is available to encrypt/decrypt with."""


def _read_key() -> bytes:
    path = Path("/run/secrets") / _SECRET_NAME
    key = path.read_text().strip() if path.is_file() else os.getenv(_ENV_VAR, "")
    if not key:
        raise SecretsNotConfigured(
            f"{_SECRET_NAME} is not configured. Generate one with "
            '`python -c "from cryptography.fernet import Fernet; '
            'print(Fernet.generate_key().decode())"`, add it to ~/.airflow as '
            f"{_ENV_VAR}, then run scripts/init-secrets.sh."
        )
    return key.encode()


_fernet = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_read_key())
    return _fernet


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext secret. Empty/None input returns "" (nothing to store)."""
    if not plaintext:
        return ""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt a ciphertext previously produced by encrypt(). Empty/None input returns ""."""
    if not ciphertext:
        return ""
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise ValueError(
            "Cannot decrypt secret — wrong webui_fernet_key, or the value isn't "
            "Fernet ciphertext (corrupted config, or a plaintext value that was "
            "never encrypted)."
        ) from exc


def is_masked(value: str) -> bool:
    """True if `value` is the placeholder the wizard shows for an unchanged secret."""
    return value == MASK


def resolve_secret(new_value: str, existing_ciphertext: str) -> str:
    """
    Decide what to persist for an editable secret field on save.

    - Blank or still-the-mask-placeholder => the user didn't change it;
      keep whatever ciphertext (if any) was already stored.
    - Anything else typed => treat it as a new plaintext secret and encrypt it.
    """
    new_value = (new_value or "").strip()
    if not new_value or is_masked(new_value):
        return existing_ciphertext or ""
    return encrypt(new_value)
