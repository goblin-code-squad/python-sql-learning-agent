#!/usr/bin/env python3
"""
explain_code.py — uses Deepseek chat/completions with "function calling" style
to request a structured JSON response containing:
  - code_explanation (string)
  - suggested_exercises (array of strings)
  - recommended_documentation (array of strings)

The script will:
- Walk the repository (skip .github and virtual envs)
- For each .py/.sql file, send the file contents to Deepseek
  with a functions schema asking the model to "return_analysis"
- Parse the model's function_call.arguments (JSON) if present,
  otherwise parse assistant message content as fallback
- Write a readable ai_explanations.txt (and print progress)
"""
import os
import json
import requests
from typing import Any, Dict, List, Optional

DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_ENDPOINT = os.getenv(
    "DEEPSEEK_ENDPOINT", "https://api.deepseek.com/chat/completions"
)

# Function schema we ask the model to call. Adjust types/fields if you want extra fields.
FUNCTIONS = [
    {
        "name": "return_analysis",
        "description": "Return a structured analysis for a code file: explanation, exercises, docs.",
        "parameters": {
            "type": "object",
            "properties": {
                "code_explanation": {
                    "type": "string",
                    "description": "A clear, concise explanation of what the code does (in English)."
                },
                "suggested_exercises": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Two or three practical exercises to practice the concepts in the code."
                },
                "recommended_documentation": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "A few URLs or titles of official docs/tutorials to study next."
                }
            },
            "required": ["code_explanation", "suggested_exercises", "recommended_documentation"]
        },
    }
]


def deepseek_request(messages: List[Dict[str, str]], model: str = "deepseek-reasoner", timeout: int = 60) -> Dict[str, Any]:
    if not DEEPSEEK_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY not found in environment")
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "functions": FUNCTIONS,
        "temperature": 0.2,
        "max_tokens": 1200,
        "stream": False,
    }
    resp = requests.post(DEEPSEEK_ENDPOINT, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_function_call_result(choice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    If the model attempted a function call, parse the JSON arguments.
    Returns dict or None.
    """
    message = choice.get("message") or {}
    func_call = message.get("function_call")
    if not func_call:
        # Some Deepseek/OpenAI-style responses put the function call at top-level choice fields
        func_call = choice.get("function_call")
    if not func_call:
        return None
    args_text = func_call.get("arguments") or ""
    try:
        parsed = json.loads(args_text)
        return parsed
    except Exception:
        # If arguments is not strict JSON, try to extract JSON block
        try:
            start = args_text.index("{")
            end = args_text.rindex("}") + 1
            return json.loads(args_text[start:end])
        except Exception:
            return None


def parse_assistant_content(choice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try to parse assistant message content as JSON or structured text.
    We expect either a JSON object or plain text with sections.
    """
    message = choice.get("message") or {}
    content = message.get("content") or choice.get("text") or ""
    if not content:
        return None
    # Attempt JSON parse
    try:
        parsed = json.loads(content)
        # If parsed object contains our expected keys, return it
        if isinstance(parsed, dict) and "code_explanation" in parsed:
            return parsed
    except Exception:
        pass

    # Fallback: try to split by headings (very rough)
    sections = {"code_explanation": "", "suggested_exercises": [], "recommended_documentation": []}
    lines = content.splitlines()
    current = "code_explanation"
    buffer: List[str] = []
    for ln in lines:
        l = ln.strip()
        if l.lower().startswith("2.") or l.lower().startswith("suggested exercises") or l.lower().startswith("exercises"):
            if buffer:
                sections[current] = "\n".join(buffer).strip()
            current = "suggested_exercises"
            buffer = []
            continue
        if l.lower().startswith("3.") or l.lower().startswith("recommended documentation") or l.lower().startswith("documentation"):
            if buffer:
                if current == "suggested_exercises":
                    # split previous buffer into items
                    items = [s.strip("-* ").strip() for s in "\n".join(buffer).splitlines() if s.strip()]
                    sections["suggested_exercises"] = items
                else:
                    sections[current] = "\n".join(buffer).strip()
            current = "recommended_documentation"
            buffer = []
            continue
        buffer.append(ln)
    # flush buffer
    if buffer:
        if current == "suggested_exercises":
            items = [s.strip("-* ").strip() for s in "\n".join(buffer).splitlines() if s.strip()]
            sections["suggested_exercises"] = items
        else:
            sections[current] = "\n".join(buffer).strip()

    # Minimal sanity check
    if sections["code_explanation"] or sections["suggested_exercises"] or sections["recommended_documentation"]:
        return sections
    return None


def explain_code(code: str, filename: str) -> Dict[str, Any]:
    """
    Returns a dict with keys: code_explanation (str), suggested_exercises (list), recommended_documentation (list)
    On error, returns a dict with code_explanation containing the error message.
    """
    system_prompt = "You are a senior code reviewer and teacher. Reply in English."
    user_prompt = (
        f"Please analyze the file named {filename}. "
        "Do NOT provide direct solutions to exercises (if the file contains exercise problems). "
        "Return structured output by 'calling' the function return_analysis with the following fields: "
        "code_explanation (string), suggested_exercises (array of 2-3 strings), "
        "recommended_documentation (array of 2-4 strings with titles or URLs). "
        "If you cannot call the function, reply with a JSON object with the same keys."
        "\n\n---\n\n" + code + "\n\n---"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        data = deepseek_request(messages)
    except Exception as e:
        return {"code_explanation": f"Deepseek request failed: {e}", "suggested_exercises": [], "recommended_documentation": []}

    # Parse the first choice
    choices = data.get("choices", [])
    if not choices:
        return {"code_explanation": f"No choices returned from Deepseek: {json.dumps(data)}", "suggested_exercises": [], "recommended_documentation": []}

    choice = choices[0]

    # 1) Try to parse function_call arguments
    parsed = parse_function_call_result(choice)
    if parsed:
        # Guarantee keys exist and are proper types
        return {
            "code_explanation": parsed.get("code_explanation", "").strip(),
            "suggested_exercises": parsed.get("suggested_exercises", []) or [],
            "recommended_documentation": parsed.get("recommended_documentation", []) or [],
        }

    # 2) Try to parse assistant content as JSON or heuristically
    parsed2 = parse_assistant_content(choice)
    if parsed2:
        return {
            "code_explanation": parsed2.get("code_explanation", "").strip() if isinstance(parsed2.get("code_explanation", ""), str) else "",
            "suggested_exercises": parsed2.get("suggested_exercises", []) or [],
            "recommended_documentation": parsed2.get("recommended_documentation", []) or [],
        }

    # 3) As last resort, include raw assistant text
    message = choice.get("message") or {}
    raw = message.get("content") or choice.get("text") or json.dumps(data)
    return {"code_explanation": f"Raw response:\n{raw}", "suggested_exercises": [], "recommended_documentation": []}


def main():
    explanations: List[str] = []
    for root, dirs, files in os.walk("."):
        # Skip .github and typical virtual env folders
        if any(skip in root for skip in [os.path.normpath(".github"), "venv", ".venv", "__pycache__"]):
            continue
        for file in files:
            if file.endswith(".py") or file.endswith(".sql"):
                path = os.path.join(root, file)
                # Skip analyzing this script to avoid recursion
                if os.path.normpath(path) == os.path.normpath("./explain_code.py"):
                    continue
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        code = f.read()
                except Exception as e:
                    explanations.append(f"## {file}\nError reading file: {e}\n")
                    continue

                print(f"Analyzing {path}...")
                result = explain_code(code, file)
                # Build a human-readable section
                block_lines = []
                block_lines.append(f"## {file}")
                block_lines.append("")
                block_lines.append("### Code Explanation")
                block_lines.append(result.get("code_explanation", "").strip() or "No explanation provided.")
                block_lines.append("")
                block_lines.append("### Suggested Exercises")
                if result.get("suggested_exercises"):
                    for idx, ex in enumerate(result["suggested_exercises"], 1):
                        block_lines.append(f"{idx}. {ex}")
                else:
                    block_lines.append("No exercises suggested.")
                block_lines.append("")
                block_lines.append("### Recommended Documentation")
                if result.get("recommended_documentation"):
                    for idx, doc in enumerate(result["recommended_documentation"], 1):
                        block_lines.append(f"{idx}. {doc}")
                else:
                    block_lines.append("No documentation suggested.")
                block_lines.append("\n---\n")
                explanations.append("\n".join(block_lines))

    if not explanations:
        explanations = ["No .py or .sql files found to analyze.\n"]

    out_path = "ai_explanations.txt"
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("\n".join(explanations))

    print(f"Wrote explanations to {out_path}")


if __name__ == "__main__":
    main()
