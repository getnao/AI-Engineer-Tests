"""
Common test utilities for AI Engineer Assessment
"""

import csv
import shutil
import subprocess
import time
from pathlib import Path


class TestEnvironment:
    """Manages test environment setup and cleanup"""

    def __init__(self, data_file_name):
        self.project_root = Path(__file__).absolute().parent / ".." / ".."
        self.data_file = self.project_root / "src" / "data" / data_file_name
        self.backup_file = self.project_root / "backup" / data_file_name
        self.agent_script = self.project_root / "agent" / "main.py"
        self.cwd = str((self.project_root / "src").resolve())

    def setup(self):
        """Setup test environment by copying backup to working file"""
        if self.backup_file.exists():
            # Copy FROM backup TO working file (candidate starts with clean data)
            shutil.copy2(self.backup_file, self.data_file)
            print(f"\t✓ Restored clean data from backup: {self.backup_file}")
            return True
        else:
            print(f"\t✗ Backup file not found: {self.backup_file}")
            return False


def find_candidate_script(agent_script_path):
    """Find the candidate's script file"""
    if agent_script_path.exists():
        return str(agent_script_path)
    return None


def run_candidate_script(script_path, prompt, cwd, timeout=60):
    """Run the candidate's script with the given prompt and working directory"""
    try:
        start_time = time.time()
        result = subprocess.run(
            ["uv", "run", "python", script_path, prompt, cwd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        end_time = time.time()
        duration = end_time - start_time

        if result.returncode != 0:
            return (
                False,
                f"Script failed with exit code {result.returncode}. Error: {result.stderr}",
                duration,
            )

        return True, result.stdout, duration

    except subprocess.TimeoutExpired:
        return False, f"Script timed out after {timeout} seconds", timeout
    except Exception as e:
        return False, f"Error running script: {e}", 0


def read_csv_data(file_path):
    """Read CSV data and return as list of dictionaries"""
    data = []
    with open(file_path, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data


def validate_price_increase(original_data, modified_data, tolerance=0.01):
    """Validate that all purchase amounts were increased by $10"""
    if len(original_data) != len(modified_data):
        return False, "Number of rows changed"

    errors = []
    for i, (orig_row, mod_row) in enumerate(zip(original_data, modified_data)):
        try:
            orig_price = float(orig_row["purchase_amount"])
            mod_price = float(mod_row["purchase_amount"])
            expected_price = orig_price + 10.0

            if abs(mod_price - expected_price) > tolerance:
                errors.append(
                    f"Row {i+1}: Expected {expected_price:.2f}, got {mod_price:.2f}"
                )
        except (ValueError, KeyError) as e:
            errors.append(f"Row {i+1}: Error parsing price - {e}")

    if errors:
        return False, "; ".join(errors[:5]) + ("..." if len(errors) > 5 else "")

    return True, "All prices correctly increased by $10"


def sample_csv_data(file_path, sample_size):
    """Sample evenly distributed rows from a large CSV file"""
    samples = []
    header = None
    total_lines = 0

    with open(file_path, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)

        for line_num, row in enumerate(reader):
            if line_num == 0:
                header = row
            else:
                # Sample evenly distributed rows
                if line_num % (1000000 // sample_size) == 1:
                    samples.append(row)
                    if len(samples) >= sample_size:
                        break
            total_lines = line_num + 1

    return header, samples, total_lines


def validate_sampled_price_increase(original_samples, modified_samples, tolerance=0.01):
    """Validate that sampled rows have prices increased by $10"""
    if len(original_samples) != len(modified_samples):
        return False, "Sample sizes don't match"

    errors = []

    for i, (orig_row, mod_row) in enumerate(zip(original_samples, modified_samples)):
        try:
            # Purchase amount is at index 5 (0-indexed)
            orig_price = float(orig_row[5])
            mod_price = float(mod_row[5])
            expected_price = orig_price + 10.0

            if abs(mod_price - expected_price) > tolerance:
                errors.append(
                    f"Sample {i+1}: Expected {expected_price:.2f}, got {mod_price:.2f}"
                )
        except (ValueError, IndexError) as error:
            errors.append(f"Sample {i+1}: Error parsing - {error}")

    if errors:
        return False, "; ".join(errors[:5]) + ("..." if len(errors) > 5 else "")

    return True, f"All {len(original_samples)} samples correctly increased by $10"
