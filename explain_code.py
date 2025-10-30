import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

def explain_code(code, filename):
    prompt = (
        f"Explain what the following {filename} file does. "
        "Do not solve exercises, but help the user understand the code. "
        "Suggest 2-3 practical exercises related to the code (for learning purposes). "
        "Recommend official documentation or tutorials for deeper study. "
        "Your answer must be in English and structured in three sections: \n"
        "1. Code Explanation\n2. Suggested Exercises\n3. Recommended Documentation\n\n"
        f"---\n{code}\n---"
    )
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an assistant that helps users learn Python and SQL by explaining code, suggesting exercises, and recommending documentation. Always reply in English."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=800,
        temperature=0.7,
    )
    return response.choices[0].message.content.strip()

def main():
    explanations = []
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.endswith(".py") or file.endswith(".sql"):
                with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                    code = f.read()
                    explanation = explain_code(code, file)
                    explanations.append(f"## {file}\n{explanation}\n")
    with open("ai_explanations.txt", "w", encoding="utf-8") as out:
        out.write("\n".join(explanations))

if __name__ == "__main__":
    main()
