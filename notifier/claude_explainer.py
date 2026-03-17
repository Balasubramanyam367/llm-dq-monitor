import anthropic
import os
from dotenv import load_dotenv

load_dotenv()


def build_prompt(failed_checks: list, dataset_name: str) -> str:
    """Turn raw validation failures into a structured prompt for Claude."""
    failures_text = "\n".join([
        f"- Column '{f['column']}': {f['expectation']} FAILED. Observed: {f['observed']}"
        for f in failed_checks
    ])

    return f"""You are a senior data engineer reviewing a data quality report.

Dataset: {dataset_name}
Total failed checks: {len(failed_checks)}

The following data quality checks FAILED:
{failures_text}

Provide a structured analysis with exactly these 3 sections:

1. ROOT CAUSE
What likely went wrong in the source system or upstream pipeline.
Be specific — name the likely system or process that caused each failure.

2. BUSINESS IMPACT
Which downstream systems, reports, or teams are affected.
What decisions could be made incorrectly because of this bad data.

3. RECOMMENDED FIX
The exact action a data engineer should take to resolve this.
Include whether this needs immediate hotfix or can wait for next pipeline run.

Keep total response under 250 words. Use plain English — no jargon."""


def explain_failures(failed_checks: list, dataset_name: str = "transactions") -> str:
    """
    Send failed DQ checks to Claude and get back a plain-English explanation.
    Returns the explanation string.
    """
    if not failed_checks:
        return "All data quality checks passed. No explanation needed."

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    prompt = build_prompt(failed_checks, dataset_name)

    print(f"Sending {len(failed_checks)} failures to Claude...")

    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=500,
        messages=[
            {"role": "user", "content": prompt}
        ],
    )

    explanation = message.content[0].text
    print("Claude responded successfully.")
    return explanation


if __name__ == "__main__":
    # Test with the exact failures from our validation run
    test_failures = [
        {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "tx_id",
            "observed": "100 null values found",
        },
        {
            "expectation": "expect_column_values_to_be_between",
            "column": "amount",
            "observed": "378 values outside [0, 10000]",
        },
        {
            "expectation": "expect_column_values_to_be_in_set",
            "column": "status",
            "observed": "466 invalid values found (e.g. ['INVALID', 'unknown'])",
        },
    ]

    explanation = explain_failures(test_failures, dataset_name="transactions")

    print("\n" + "="*60)
    print("CLAUDE'S EXPLANATION:")
    print("="*60)
    print(explanation)
    print("="*60)