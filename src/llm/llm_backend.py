import json
import subprocess
import sys
from typing import Any, Dict


def _extract_first_json(text: str) -> Dict[str, Any]:
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in model output.")

    depth = 0
    end = -1
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break

    if end == -1:
        raise ValueError("No complete JSON object found in model output.")

    candidate = text[start : end + 1]
    try:
        obj = json.loads(candidate)
    except Exception as exc:
        raise ValueError(f"Failed to parse JSON object: {exc}") from exc

    if not isinstance(obj, dict):
        raise ValueError("Parsed JSON is not an object.")
    return obj


def label_with_llama(prompt: str) -> Dict[str, Any]:
    schema = (
        '{"type":"object","properties":{'
        '"category":{"type":"string"},'
        '"label_severity":{"type":["string","integer","number"]},'
        '"label_direction":{"type":"string"},'
        '"label_time_horizon_1_min":{"type":["string","integer","number","null"]},'
        '"label_time_horizon_2_min":{"type":["string","integer","number","null"]},'
        '"confidence":{"type":["string","number","null"]},'
        '"needs_review":{"type":["string","boolean","null"]}'
        '},'
        '"required":["category","label_severity","label_direction"]}'
    )
    result = subprocess.run(
        [
            "llama-cli",
            "-m",
            "models/llama-3.1-8b.gguf",
            "--temp",
            "0",
            "--n-predict",
            "256",
            "--single-turn",
            "--simple-io",
            "--no-display-prompt",
            "--log-disable",
            "-ngl",
            "0",
            "--no-mmap",
            "--json-schema",
            schema,
            "-p",
            prompt,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = result.stdout or ""
    stderr = result.stderr or ""

    text = stdout if stdout.strip() else stderr
    if not text.strip():
        raise RuntimeError(f"llama-cli produced no output (code {result.returncode}).")

    try:
        return _extract_first_json(text)
    except Exception:
        if not hasattr(label_with_llama, "_printed_debug"):
            print({"llama_returncode": result.returncode, "stderr": stderr[:200]}, file=sys.stderr)
            label_with_llama._printed_debug = True
        return text


if __name__ == "__main__":
    parsed = label_with_llama('Return ONLY valid JSON: {"ok": true}')
    print(parsed)
