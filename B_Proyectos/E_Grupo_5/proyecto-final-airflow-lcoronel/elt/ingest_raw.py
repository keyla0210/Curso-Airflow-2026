import json
import logging
from pathlib import Path
from typing import List, Sequence
import requests

logger = logging.getLogger(__name__)

def ingest_to_raw_simple(
    api_url: str,
    output_dir: Path | str,
    timeout: int = 30,
) -> str:

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.error("Error al descargar el JSON: %s", exc)
        return ""

    file_path = output_dir / "sucesos_maritimos.json"
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    return str(file_path)
