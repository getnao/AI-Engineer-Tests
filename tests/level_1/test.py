"""
Level 1 Test Script
Tests that the candidate's AI agent can modify a small CSV file correctly.
"""

import sys
from pathlib import Path

# Add parent directory to path to access common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.test_utils import (
    TestEnvironment,
    find_candidate_script,
    run_candidate_script,
    read_csv_data,
    validate_price_increase,
)


def main():
    print("🧪 Level 1 Test: Basic File Modification Agent")

    # Setup test environment
    env = TestEnvironment("data_1.csv")
    env.setup()

    try:
        # Find candidate script
        script_path = find_candidate_script(env.agent_script)
        if not script_path:
            raise Exception(
                "No candidate script found (looking for agent/main.py in project root)"
            )

        print(f"\t✓ Found candidate script: {script_path}")

        # Read original data (from backup to know expected starting state)
        original_data = read_csv_data(env.backup_file)
        print(f"\t✓ Read original data: {len(original_data)} rows")

        # Run candidate script
        prompt = "\tPlease add $10 to all purchase amounts in 'data/data_1.csv' because there was an error in recording"
        print(f"\t✓ Running script with prompt: '{prompt}'")
        print(f"\t✓ Working directory: {env.cwd}")
        print("\n")

        success, output, duration = run_candidate_script(script_path, prompt, env.cwd)
        if not success:
            raise Exception(f"Script execution failed: {output}")

        print(f"✓ Script completed successfully in {duration:.2f}s")
        if output.strip():
            print(f"   Output: {output.strip()}")

        # Validate results (compare modified file against original backup data)
        modified_data = read_csv_data(env.data_file)
        valid, message = validate_price_increase(original_data, modified_data, 0.1)

        if valid:
            print(f"✅ TEST PASSED: {message}")
            env.cleanup()
            sys.exit(0)
        else:
            raise Exception(message)

    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
