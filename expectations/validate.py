import pandas as pd
import boto3
import os
from dotenv import load_dotenv

load_dotenv()


def download_from_s3() -> pd.DataFrame:
    """Download latest CSV from S3 and return as DataFrame."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    local_path = "data/transactions.csv"
    s3.download_file(
        os.getenv("S3_BUCKET"),
        "raw/transactions/transactions.csv",
        local_path,
    )
    print("Downloaded transactions.csv from S3")
    return pd.read_csv(local_path)


def run_validation(df: pd.DataFrame) -> dict:
    """
    Run data quality checks on the DataFrame.
    Returns:
      - passed: bool
      - failed_checks: list of dicts describing each failure
      - summary: str
      - total_checks: int
    """
    failed_checks = []
    total_checks = 0

    def check(name: str, column: str, condition: bool, observed: str):
        """Register one DQ check result."""
        nonlocal total_checks
        total_checks += 1
        if not condition:
            failed_checks.append({
                "expectation": name,
                "column":      column,
                "observed":    observed,
            })

    # ----------------------------------------------------------
    # RULE 1 — tx_id must never be null (primary key)
    # ----------------------------------------------------------
    null_tx = df["tx_id"].isna().sum()
    check(
        name="expect_column_values_to_not_be_null",
        column="tx_id",
        condition=null_tx == 0,
        observed=f"{null_tx} null values found",
    )

    # ----------------------------------------------------------
    # RULE 2 — amount must never be null
    # ----------------------------------------------------------
    null_amt = df["amount"].isna().sum()
    check(
        name="expect_column_values_to_not_be_null",
        column="amount",
        condition=null_amt == 0,
        observed=f"{null_amt} null values found",
    )

    # ----------------------------------------------------------
    # RULE 3 — amount must be between 0 and 10,000
    # ----------------------------------------------------------
    bad_amt = df["amount"].dropna()
    out_of_range = ((bad_amt < 0) | (bad_amt > 10000)).sum()
    check(
        name="expect_column_values_to_be_between",
        column="amount",
        condition=out_of_range == 0,
        observed=f"{out_of_range} values outside [0, 10000]",
    )

    # ----------------------------------------------------------
    # RULE 4 — status must only contain valid values
    # ----------------------------------------------------------
    valid_statuses = {"success", "failed", "pending"}
    bad_status = (~df["status"].isin(valid_statuses)).sum()
    check(
        name="expect_column_values_to_be_in_set",
        column="status",
        condition=bad_status == 0,
        observed=f"{bad_status} invalid values found "
                 f"(e.g. {df[~df['status'].isin(valid_statuses)]['status'].unique()[:3].tolist()})",
    )

    # ----------------------------------------------------------
    # RULE 5 — user_id must never be null
    # ----------------------------------------------------------
    null_user = df["user_id"].isna().sum()
    check(
        name="expect_column_values_to_not_be_null",
        column="user_id",
        condition=null_user == 0,
        observed=f"{null_user} null values found",
    )

    # ----------------------------------------------------------
    # RULE 6 — timestamp must never be null
    # ----------------------------------------------------------
    null_ts = df["timestamp"].isna().sum()
    check(
        name="expect_column_values_to_not_be_null",
        column="timestamp",
        condition=null_ts == 0,
        observed=f"{null_ts} null values found",
    )

    # ----------------------------------------------------------
    # RULE 7 — is_international must only be 0 or 1
    # ----------------------------------------------------------
    bad_intl = (~df["is_international"].isin([0, 1])).sum()
    check(
        name="expect_column_values_to_be_in_set",
        column="is_international",
        condition=bad_intl == 0,
        observed=f"{bad_intl} values outside [0, 1]",
    )

    # ----------------------------------------------------------
    # RULE 8 — row count must be between 100 and 1,000,000
    # ----------------------------------------------------------
    row_count = len(df)
    check(
        name="expect_table_row_count_to_be_between",
        column="table",
        condition=100 <= row_count <= 1_000_000,
        observed=f"{row_count} rows",
    )

    # ----------------------------------------------------------
    # RULE 9 — all required columns must exist
    # ----------------------------------------------------------
    required_cols = ["tx_id", "amount", "user_id",
                     "timestamp", "status", "merchant", "is_international"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    check(
        name="expect_table_columns_to_exist",
        column="table",
        condition=len(missing_cols) == 0,
        observed=f"Missing columns: {missing_cols}" if missing_cols else "all present",
    )

    # ----------------------------------------------------------
    # Summary
    # ----------------------------------------------------------
    passed = len(failed_checks) == 0
    summary = (
        f"All {total_checks} checks passed."
        if passed
        else f"{len(failed_checks)} of {total_checks} checks FAILED."
    )

    print(summary)
    for f in failed_checks:
        print(f"  FAIL — {f['column']}: {f['expectation']} | {f['observed']}")

    return {
        "passed":        passed,
        "failed_checks": failed_checks,
        "summary":       summary,
        "total_checks":  total_checks,
    }


if __name__ == "__main__":
    df = download_from_s3()
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    result = run_validation(df)

    if result["passed"]:
        print("\n All checks passed — no alerts needed.")
    else:
        print(f"\n {result['summary']}")
        print("\nFailed checks ready to send to Claude:")
        for f in result["failed_checks"]:
            print(f"  - [{f['column']}] {f['expectation']}: {f['observed']}")