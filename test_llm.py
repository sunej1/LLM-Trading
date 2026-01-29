from openai import OpenAI

client = OpenAI()  # automatically reads OPENAI_API_KEY

resp = client.responses.create(
    model="gpt-4.1-mini",
    input="Say hello in one sentence."
)

print(resp.output_text)