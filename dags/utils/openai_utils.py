from openai import AzureOpenAI
from airflow.decorators import task
import os
import json

from dotenv import load_dotenv
load_dotenv()

endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
model_name = os.getenv("AZURE_OPENAI_MODEL")
deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
api_version = os.getenv("AZURE_OPENAI_API_VERSION")
subscription_key = os.getenv("AZURE_OPENAI_KEY")

print(f"Using Azure OpenAI endpoint: {endpoint}, model: {model_name}, deployment: {deployment}, api_version: {api_version}")

@task
def parse_file_with_openai(file_path: str, prompt: str) -> str:
    """
    Reads the file at file_path, sends its content to OpenAI with the given prompt, and returns the response.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "r") as f:
        file_content = f.read()
    if not subscription_key:
        raise ValueError("OpenAI API key not set.")
    
    openai = AzureOpenAI(
        azure_endpoint=endpoint,
        api_version=api_version,
        api_key=subscription_key,
    )

    response = openai.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": file_content},
        ],
        temperature=0,
    )
    
    raw_openai_response_content = response.choices[0].message.content

    cleaned_content = raw_openai_response_content.strip()
    if cleaned_content.startswith('```json'):
        cleaned_content = cleaned_content[len('```json'):]
    elif cleaned_content.startswith('```'): # Handle cases where it's just ```
        cleaned_content = cleaned_content[len('```'):]
    if cleaned_content.endswith('```'):
        cleaned_content = cleaned_content[:-len('```')]
    
    cleaned_content = cleaned_content.strip()

    try:
        parsed_data_dict = json.loads(cleaned_content)
    except json.JSONDecodeError as e:
        print(f"Error: OpenAI response content is not valid JSON AFTER CLEANING: {cleaned_content}")
        raise ValueError(f"OpenAI did not return valid JSON after cleaning: {e}")
    
    return json.dumps(parsed_data_dict)