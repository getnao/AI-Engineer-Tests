"""
Level 2 Test Script
Tests that the candidate's AI agent can modify a large CSV file (1M rows) correctly.
"""

import sys
from pathlib import Path

# Add parent directory to path to access common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.test_utils import (
    TestEnvironment,
    find_candidate_script,
    run_candidate_script,
    sample_csv_data,
    validate_sampled_price_increase,
)

SAMPLE_SIZE = 1000  # Sample rows to validate


def main():
    print("🧪 Level 2 Test: Large File Processing Agent")

    # Setup test environment
    env = TestEnvironment("data_2.csv")
    env.setup()

    try:
        # Find candidate script
        script_path = find_candidate_script(env.agent_script)
        if not script_path:
            raise Exception(
                "No candidate script found (looking for agent/main.py in project root)"
            )

        print(f"\t✓ Found candidate script: {script_path}")

        # Sample original data (from backup to know expected starting state)
        print(f"\t✓ Sampling original data ({SAMPLE_SIZE} rows)...")
        original_header, original_samples, total_lines = sample_csv_data(
            env.backup_file, SAMPLE_SIZE
        )
        print(f"\t✓ Original file has {total_lines} lines")

        # Run candidate script
        prompt = "\tPlease add $10 to all purchase amounts in 'data/data_2.csv' because there was an error in recording"
        print(f"\t✓ Running script with prompt: '{prompt}'")
        print(f"\t✓ Working directory: {env.cwd}")
        print("\t(This may take several minutes for large file processing...)")
        print("\n")

        success, output, duration = run_candidate_script(
            script_path, prompt, env.cwd, timeout=300
        )

        if not success:
            raise Exception(f"Script execution failed: {output}")

        print(f"✓ Script completed successfully in {duration:.2f}s")
        if output.strip():
            print(f"   Output: {output.strip()}")

        # Validate results by sampling (compare modified file against original backup data)
        print(f"✓ Validating results by sampling {SAMPLE_SIZE} rows...")
        modified_header, modified_samples, modified_total_lines = sample_csv_data(
            env.data_file, SAMPLE_SIZE
        )

        if modified_total_lines != total_lines:
            raise Exception(
                f"Line count changed from {total_lines} to {modified_total_lines}"
            )

        valid, message = validate_sampled_price_increase(
            original_samples, modified_samples
        )

        if valid:
            print(f"✅ TEST PASSED: {message}")
            print(f"   Processing time: {duration:.2f}s")
            print(f"   File size: {env.data_file.stat().st_size // (1024 * 1024)}MB")
            env.cleanup()
            sys.exit(0)
        else:
            raise Exception(message)

    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
