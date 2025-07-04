from google import genai

GEMINI_API_KEY = "GEMINI_API_KEY"

def generate():
    client = genai.Client(
        api_key=GEMINI_API_KEY,
    )

    model = "gemini-2.5-flash-preview-05-20"

    file_x = client.files.upload(file='cnbc_news.csv')

    response = client.models.generate_content(
        model=model, contents=["Summarize news from this file? in 100 word", file_x]
    )
    print(response.text)


if __name__ == "__main__":
    generate()
