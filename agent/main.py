"""
AI Agent for Nao Code Editor
Handles user prompts and modifies codebases accordingly.
"""

import os
import sys
from openai import OpenAI


def main():
    """Main entry point for the AI agent"""
    if len(sys.argv) != 3:
        print("Usage: python agent/main.py <prompt> <working_directory>")
        sys.exit(1)

    prompt = sys.argv[1]
    user_cwd = sys.argv[2]

    print(f"[Prompt]: {prompt}")
    print(f"[User CWD]: {user_cwd}\n")

    # Initialize OpenAI client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable is not set")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    try:
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=[{"role": "user", "content": prompt}],
        )

        # Extract and print the response
        ai_response = response.choices[0].message.content
        print(ai_response)

    except Exception as e:
        print(f"Error making OpenAI request: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
