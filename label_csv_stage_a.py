import argparse
import csv
import json
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from llm_backend import label_with_llama


VALID_DIRECTIONS = {"positive", "negative", "mixed", "neutral"}


def build_prompt(row: Dict[str, str]) -> str:
    """Construct a compact prompt instructing the model to return only JSON."""
    parts = [
        "Return ONLY valid JSON with keys: category, label_severity, label_direction, "
        "label_time_horizon_1_min, label_time_horizon_2_min, confidence, needs_review.",
        "Severity: 0 (none) to 5 (very high).",
        "Direction: one of positive, negative, mixed, neutral.",
        "Time horizons: non-negative minutes; label_time_horizon_2_min may be null if not applicable.",
        "Context:",
        f"timestamp: {row.get('timestamp', '')}",
        f"ticker: {row.get('ticker', '')}",
        f"ticker_confidence: {row.get('ticker_confidence', '')}",
        f"source_credibility: {row.get('source_credibility', '')}",
        f"headline: {row.get('headline', '')}",
    ]
    text_val = row.get("text", "")
    if text_val:
        parts.append(f"text: {text_val}")

    excerpt_val = row.get("article_excerpt", "")
    if excerpt_val:
        parts.append(f"article_excerpt: {excerpt_val}")

    return "\n".join(parts)


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        try:
            return int(value.strip())
        except Exception:
            return None
    return None


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except Exception:
            return None
    return None


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _extract_first_json_str(text: str) -> Dict[str, Any]:
    start = text.find("{")
    if start == -1:
        return {}
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
        return {}
    snippet = text[start : end + 1]
    try:
        obj = json.loads(snippet)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return {}
    return {}


def validate_output(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Validate model output; return cleaned data or None and review flag."""
    try:
        category = str(raw.get("category", "")).strip()
        direction = str(raw.get("label_direction", "")).strip().lower()

        severity_raw = raw.get("label_severity", "")
        severity_val = _to_int(severity_raw)

        t1_val = raw.get("label_time_horizon_1_min")
        if t1_val is None or (isinstance(t1_val, str) and t1_val.strip().lower() == "null") or t1_val == "":
            t1_out: Any = ""
        else:
            t1_num = _to_int(t1_val)
            t1_out = "" if t1_num is None else t1_num

        t2_val = raw.get("label_time_horizon_2_min")
        if t2_val is None or (isinstance(t2_val, str) and t2_val.strip().lower() == "null") or t2_val == "":
            t2_out: Any = ""
        else:
            t2_num = _to_int(t2_val)
            t2_out = "" if t2_num is None else t2_num

        confidence_raw = raw.get("confidence", "")
        confidence_val = _to_float(confidence_raw)
        confidence_out = "" if confidence_val is None else confidence_val

        needs_review_val = raw.get("needs_review")
        needs_review = _parse_bool(needs_review_val)

        if not category or severity_val is None or severity_val < 0 or severity_val > 5 or direction not in VALID_DIRECTIONS:
            return None, True

        cleaned = {
            "category": category,
            "label_severity": severity_val,
            "label_direction": direction,
            "label_time_horizon_1_min": t1_out,
            "label_time_horizon_2_min": t2_out,
            "label_confidence": confidence_out,
            "label_needs_review": needs_review,
        }
        return cleaned, needs_review
    except Exception:
        return None, True


def process_rows(rows: List[Dict[str, str]], limit: Optional[int]) -> List[Dict[str, str]]:
    printed_debug = False
    processed: List[Dict[str, str]] = []
    for idx, row in enumerate(rows):
        updated = row.copy()

        if limit is not None and idx >= limit:
            # Pass through rows beyond the limit unchanged, ensuring required columns exist and flagged for review.
            updated.setdefault("label_confidence", "")
            updated["label_needs_review"] = "true"
            processed.append(updated)
            continue

        try:
            prompt = build_prompt(row)
            model_output = label_with_llama(prompt)
            if not printed_debug:
                print({"model_output": model_output})
            if isinstance(model_output, dict) and isinstance(model_output.get("parameters"), dict):
                labels = model_output["parameters"]
            elif isinstance(model_output, str):
                parsed = {}
                try:
                    parsed = json.loads(model_output)
                except Exception:
                    parsed = _extract_first_json_str(model_output)
                if isinstance(parsed, dict) and isinstance(parsed.get("parameters"), dict):
                    labels = parsed["parameters"]
                elif isinstance(parsed, dict):
                    labels = parsed
                else:
                    labels = {}
            else:
                labels = model_output if isinstance(model_output, dict) else {}
            updated["category"] = labels.get("category", "")
            updated["label_severity"] = labels.get("label_severity", "")
            updated["label_direction"] = labels.get("label_direction", "")
            updated["label_time_horizon_1_min"] = "" if labels.get("label_time_horizon_1_min") in [None, "null"] else labels.get("label_time_horizon_1_min")
            updated["label_time_horizon_2_min"] = "" if labels.get("label_time_horizon_2_min") in [None, "null"] else labels.get("label_time_horizon_2_min")
            updated["label_confidence"] = labels.get("confidence", "")
            updated["label_needs_review"] = str(labels.get("needs_review", "false")).lower()
            if not printed_debug:
                print(
                    {
                        "category": updated.get("category"),
                        "label_severity": updated.get("label_severity"),
                        "label_direction": updated.get("label_direction"),
                        "label_time_horizon_1_min": updated.get("label_time_horizon_1_min"),
                        "label_time_horizon_2_min": updated.get("label_time_horizon_2_min"),
                        "label_confidence": updated.get("label_confidence"),
                        "label_needs_review": updated.get("label_needs_review"),
                    },
                    file=sys.stderr,
                )
                printed_debug = True
        except Exception as exc:
            if not printed_debug:
                print({"error": str(exc)})
                printed_debug = True
            updated["category"] = updated.get("category", "")
            updated["label_severity"] = updated.get("label_severity", "")
            updated["label_direction"] = updated.get("label_direction", "")
            updated["label_time_horizon_1_min"] = updated.get("label_time_horizon_1_min", "")
            updated["label_time_horizon_2_min"] = updated.get("label_time_horizon_2_min", "")
            updated["label_confidence"] = updated.get("label_confidence", "")
            updated["label_needs_review"] = "true"

        processed.append(updated)
    return processed


def _sanitize_row_strings(row: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = {}
    for k, v in row.items():
        if isinstance(v, str):
            cleaned = re.sub(r"\s+", " ", v.replace("\r", " ").replace("\n", " ")).strip()
            sanitized[k] = cleaned
        else:
            sanitized[k] = v
    return sanitized


def main() -> None:
    parser = argparse.ArgumentParser(description="Label combined CSV rows using local LLaMA.")
    parser.add_argument("--in", dest="input_path", required=True, help="Input combined CSV path.")
    parser.add_argument("--out", dest="output_path", required=True, help="Output labeled CSV path.")
    parser.add_argument("--limit", dest="limit", type=int, default=None, help="Optional max rows to process.")
    args = parser.parse_args()

    with open(args.input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fieldnames = reader.fieldnames or []
        rows = list(reader)

    # Preserve existing order; append new columns if missing.
    output_fieldnames = list(input_fieldnames)
    for field in ["label_confidence", "label_needs_review"]:
        if field not in output_fieldnames:
            output_fieldnames.append(field)

    processed_rows = process_rows(rows, args.limit)

    print(f"Writing {len(processed_rows)} rows to {args.output_path}", file=sys.stderr)

    sanitized_rows = []
    for row in processed_rows:
        row["category"] = str(row.get("category", ""))
        row["label_severity"] = str(row.get("label_severity", ""))
        row["label_direction"] = str(row.get("label_direction", ""))
        row["label_time_horizon_1_min"] = str(row.get("label_time_horizon_1_min", ""))
        row["label_time_horizon_2_min"] = str(row.get("label_time_horizon_2_min", ""))
        row.setdefault("label_confidence", "")
        row["label_needs_review"] = "true" if str(row.get("label_needs_review", "true")).lower() in {"", "true", "1", "yes"} else "false"
        sanitized_rows.append(_sanitize_row_strings(row))

    with open(args.output_path, "w", newline="", encoding="utf-8") as f:
        print(f"FIELDNAMES={output_fieldnames}", file=sys.stderr)
        writer = csv.DictWriter(f, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(sanitized_rows)


if __name__ == "__main__":
    main()
