from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

import pandas as pd

try:
    import yaml  
except ImportError as e:
    raise SystemExit("Missing dependency: PyYAML. Install with: pip install pyyaml") from e


STANDARD_COLUMNS = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]


def setup_logger(log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("eligibility_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def format_phone(raw: str, digits_expected: int = 10) -> Optional[str]:
    d = digits_only(raw)
    if len(d) != digits_expected:
        return None
    return f"{d[0:3]}-{d[3:6]}-{d[6:10]}"


def to_title_case(s: str) -> str:
    
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip().title()


def safe_lower(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip().lower()


def parse_dob(series: pd.Series, fmt: str, logger: logging.Logger) -> pd.Series:
    dt = pd.to_datetime(series, format=fmt, errors="coerce")
    bad = dt.isna() & series.notna() & (series.astype(str).str.strip() != "")
    bad_count = int(bad.sum())
    if bad_count:
        logger.warning("DOB parse: %s rows could not be parsed with format %s; setting dob=NULL for those rows.",
                       bad_count, fmt)
    return dt.dt.strftime("%Y-%m-%d")


def read_partner_file(path: str, delimiter: str, has_header: bool, logger: logging.Logger) -> pd.DataFrame:
    header = 0 if has_header else None
    
    try:
        df = pd.read_csv(
            path,
            sep=delimiter,
            header=header,
            dtype=str,
            keep_default_na=False,  
            on_bad_lines="skip",
            engine="python",
        )
    except TypeError:
        
        df = pd.read_csv(
            path,
            sep=delimiter,
            header=header,
            dtype=str,
            keep_default_na=False,
            engine="python",
            error_bad_lines=False,  
            warn_bad_lines=True,    
        )
    logger.info("Read %s rows from %s", len(df), os.path.basename(path))
    return df


def standardize_partner_df(raw: pd.DataFrame, partner_cfg: Dict[str, Any], logger: logging.Logger) -> pd.DataFrame:
    mapping: Dict[str, str] = partner_cfg["column_mapping"]

    
    selected = {}
    for partner_col, std_col in mapping.items():
        if partner_col in raw.columns:
            selected[std_col] = raw[partner_col]
        else:
            logger.warning("Missing column '%s' for partner_code=%s; filling with empty values.",
                           partner_col, partner_cfg["partner_code"])
            selected[std_col] = ""

    df = pd.DataFrame(selected)

    
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    
    df["external_id"] = df["external_id"].astype(str).str.strip()
    df["first_name"] = df["first_name"].apply(to_title_case)
    df["last_name"] = df["last_name"].apply(to_title_case)

    dob_fmt = partner_cfg.get("date_parse", {}).get("format")
    if dob_fmt:
        df["dob"] = parse_dob(df["dob"], dob_fmt, logger)
    else:
        df["dob"] = pd.to_datetime(df["dob"], errors="coerce").dt.strftime("%Y-%m-%d")

    df["email"] = df["email"].apply(safe_lower)

    digits_expected = int(partner_cfg.get("phone_parse", {}).get("digits_expected", 10))
    df["phone"] = df["phone"].apply(lambda x: format_phone(x, digits_expected=digits_expected))

    df["partner_code"] = partner_cfg["partner_code"]

    
    before = len(df)
    df = df[df["external_id"].astype(str).str.strip() != ""].copy()
    dropped = before - len(df)
    if dropped:
        logger.warning("Validation: dropped %s rows missing external_id for partner_code=%s",
                       dropped, partner_cfg["partner_code"])

    
    return df[STANDARD_COLUMNS]


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def write_outputs(
    unified: pd.DataFrame,
    output_dir: str,
    output_format: str,
    logger: logging.Logger,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    if output_format.lower() == "csv":
        out_path = os.path.join(output_dir, "eligibility_unified.csv")
        unified.to_csv(out_path, index=False)
        logger.info("Wrote CSV to %s", out_path)
        return

    if output_format.lower() in {"parquet", "delta"}:
        
        try:
            from pyspark.sql import SparkSession  
            spark = SparkSession.builder.appName("eligibility_pipeline").getOrCreate()
            sdf = spark.createDataFrame(unified.astype(str))
            out_path = os.path.join(output_dir, f"eligibility_unified.{output_format.lower()}")
            if output_format.lower() == "parquet":
                sdf.write.mode("overwrite").parquet(out_path)
                logger.info("Wrote Parquet to %s (Spark)", out_path)
            else:
                
                
                sdf.write.format("delta").mode("overwrite").save(out_path)
                logger.info("Wrote Delta to %s (Spark)", out_path)
            return
        except Exception as e:
            logger.warning("Spark write failed (%s). Falling back to CSV.", str(e))

        
        out_path = os.path.join(output_dir, "eligibility_unified.csv")
        unified.to_csv(out_path, index=False)
        logger.info("Wrote CSV fallback to %s", out_path)
        return

    raise ValueError(f"Unsupported output_format: {output_format}. Use csv, parquet, or delta.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Configuration-driven eligibility ingestion pipeline")
    parser.add_argument("--config", required=True, help="Path to partners YAML config")
    parser.add_argument("--input_dir", required=True, help="Directory containing partner input files")
    parser.add_argument("--output_dir", required=True, help="Directory to write outputs")
    parser.add_argument("--output_format", default="csv", choices=["csv", "parquet", "delta"], help="Output format")
    parser.add_argument("--log_path", default=os.path.join("logs", "pipeline.log"), help="Path to log file")
    args = parser.parse_args()

    logger = setup_logger(args.log_path)
    cfg = load_config(args.config)

    unified_parts: List[pd.DataFrame] = []

    partners = cfg.get("partners", {})
    if not partners:
        raise SystemExit("No partners found in config. Expected key: partners")

    for partner_name, partner_cfg in partners.items():
        file_name = partner_cfg["file_name"]
        in_path = os.path.join(args.input_dir, file_name)

        if not os.path.exists(in_path):
            logger.error("Missing input file for partner '%s': %s", partner_name, in_path)
            continue

        raw = read_partner_file(
            path=in_path,
            delimiter=partner_cfg.get("delimiter", ","),
            has_header=bool(partner_cfg.get("has_header", True)),
            logger=logger,
        )
        standardized = standardize_partner_df(raw, partner_cfg, logger)
        unified_parts.append(standardized)

        logger.info("Standardized %s rows for partner '%s' (%s)",
                    len(standardized), partner_name, partner_cfg.get("partner_code"))

    if not unified_parts:
        raise SystemExit("No partner data processed. Check input_dir and config.")

    unified = pd.concat(unified_parts, ignore_index=True)
    logger.info("Unified dataset rows: %s", len(unified))

    write_outputs(unified, args.output_dir, args.output_format, logger)


if __name__ == "__main__":
    main()
