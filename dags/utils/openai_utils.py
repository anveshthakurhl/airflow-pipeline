from openai import AzureOpenAI
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import os
import json
from .openai_prompts import (
    OPENAI_CLINICAL_NOTES_VALIDATION_PROMPT,
    OPENAI_FACTORS_VALIDATION_PROMPT,
    OPENAI_RATING_SCALES_VALIDATION_PROMPT,
)

from dotenv import load_dotenv
load_dotenv()

# endpoint = os.getenv("AZURE_OPENAI_O3_ENDPOINT")
# model_name = os.getenv("AZURE_OPENAI_O3_MODEL")
# deployment = os.getenv("AZURE_OPENAI_O3_DEPLOYMENT")
# api_version = os.getenv("AZURE_OPENAI_O3_API_VERSION")
# subscription_key = os.getenv("AZURE_OPENAI_O3_KEY")

endpoint = os.getenv("AZURE_OPENAI_5_ENDPOINT")
model_name = os.getenv("AZURE_OPENAI_5_MODEL")
deployment = os.getenv("AZURE_OPENAI_5_DEPLOYMENT")
api_version = os.getenv("AZURE_OPENAI_5_API_VERSION")
subscription_key = os.getenv("AZURE_OPENAI_5_KEY")

# endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
# model_name = os.getenv("AZURE_OPENAI_MODEL")
# deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
# api_version = os.getenv("AZURE_OPENAI_API_VERSION")
# subscription_key = os.getenv("AZURE_OPENAI_KEY")


FACTORS = [
    ("Anhedonia", "Little interest or pleasure in doing things?"),
    ("Depressed Mood", "Feeling down, depressed, or hopeless?"),
    ("Fatigue / Low Energy", "Feeling tired or having little energy?"),
    ("Appetite / Weight Change", "Poor appetite or overeating?"),
    ("Self-worth / Guilt", "Feeling bad about yourself—or that you are a failure or have let yourself or your family down?"),
    ("Concentration Difficulties", "Trouble concentrating on things, such as reading the newspaper or watching television?"),
    ("Psychomotor Changes", "Moving or speaking so slowly that other people could have noticed? Or the opposite—being so fidgety or restless that you have been moving around a lot more than usual?"),
    ("Psychiatric Co-Morbidities", "Are there any additional psychiatric diagnoses, symptoms, or comorbid conditions (e.g., anxiety, PTSD, OCD)?"),
    ("Medication Side Effects", "Are there any indications that current medications are causing side effects?"),
    ("Presenting Problem", "What is the patient’s chief complaint or reason for visit today?"),
    ("Alcohol Consumption", "Any mention of alcohol use, pattern, quantity, or its effect?"),
    ("Appearance & Behaviour", "Describe the patient's appearance and observed behavior (e.g., calm, restless, disheveled)."),
    ("Speech and Language", "Any observations about how the patient speaks (rate, rhythm, volume, coherence)?"),
    ("Thought Process", "How is the patient's flow of thought described (e.g., linear, tangential, disorganized)?"),
    ("Cognition", "Any comment on memory, attention, orientation, or cognitive functioning?"),
    ("Insight and Judgment", "How well is the patient's awareness and decision-making described (e.g., good, poor, fair)?"),
    ("Suicidal Ideation", "Any thoughts of self-harm or suicidal ideation expressed by the patient?"),
    ("Homicidal Ideation", "Any mention of thoughts or intent to harm others?"),
    ("Life Events / Stressors", "Any mention of recent life events, stressors, or significant changes?"),
    ("Lifestyle / Substance Abuse History", "Any information about lifestyle choices, substance use, or abuse history?"),
    ("Sleep/Activity Data (Patient Reported)", "Any sleep or activity-related data reported by the patient?"),
    ("Medication History", "Any information about the patient's past or current medication history?"),
    ("Patient's Family History", "Any mention of the patient's family medical or psychiatric history?"),
    ("Hospitalization", "Any information about past or current hospitalizations, including reason, date, and duration?"),
]

@task
def parse_file_with_openai(file_content: str, prompt: str) -> str:
    """
    Reads the file at file_path, sends its content to OpenAI with the given prompt, and returns the response.
    Also calls factor_extraction and merges its output under 'mental_health_factors'.
    """
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
        # temperature=0,
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

    # Call factor_extraction and merge results into top-level JSON
    factors_json_str = factor_extraction(file_content)
    try:
        factors_data = json.loads(factors_json_str)
    except Exception as e:
        print(f"Error parsing factor_extraction output: {factors_json_str}")
        factors_data = None

    # If factors_data is a list, merge each factor into the main dict
    if isinstance(factors_data, list):
        for factor_obj in factors_data:
            factor_name = factor_obj.get("factor")
            if factor_name:
                parsed_data_dict[factor_name] = {
                    k: v for k, v in factor_obj.items() if k != "factor"
                }
    else:
        parsed_data_dict["mental_health_factors"] = factors_data

    # Call extract_rating_scales and merge results into top-level JSON
    rating_scales_json_str = extract_rating_scales(file_content)
    try:
        rating_scales_data = json.loads(rating_scales_json_str)
    except Exception as e:
        print(f"Error parsing extract_rating_scales output: {rating_scales_json_str}")
        rating_scales_data = None

    if rating_scales_data and "rating_scales" in rating_scales_data:
        parsed_data_dict["rating_scales"] = rating_scales_data["rating_scales"]

    return json.dumps(parsed_data_dict)

def factor_extraction(clinical_note: str) -> str:
    """
    Extracts mental health factors from a clinical note using OpenAI.
    Returns a JSON string of extracted factors.
    """
    if not subscription_key:
        raise ValueError("OpenAI API key not set.")

    system_prompt = (
        "You are a helpful assistant that extracts structured mental health information from clinical notes. "
        "Your task is to extract only the information that is explicitly stated in the note, without inferring, "
        "guessing, or fabricating details. "
        "If a factor is not directly mentioned in the note, mark isPresent as false and leave 'instance' as an empty string. "
        "Do not use external medical knowledge to fill gaps. "
        "Only use quotes or concise summaries that are clearly present in the source text. "
        "Return a JSON object with:\n"
        "- factor: name of the symptom or attribute\n"
        "- instance: an exact quote or short paraphrase from the note if available, otherwise an empty string\n"
        "- isPresent: true only if the note explicitly mentions or clearly confirms the factor; otherwise false."
    )

    factors_formatted = "\n".join(
        [f'{i+1}. {name} - "{question}"' for i, (name, question) in enumerate(FACTORS)]
    )

    user_prompt = (
        f"Given the following clinical note, extract each of the 19 factors below and answer whether they are present, with supporting text if available.\n\n"
        f"### Factors:\n{factors_formatted}\n\n"
        f"### Clinical Note:\n{clinical_note}\n\n"
        "Return a list of JSON objects like:\n 'factor': {'factor': '', 'instance': '', 'isPresent': true}"
    )

    openai = AzureOpenAI(
        azure_endpoint=endpoint,
        api_version=api_version,
        api_key=subscription_key,
    )

    response = openai.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        # temperature=0,
    )

    raw_openai_response_content = response.choices[0].message.content
    cleaned_content = raw_openai_response_content.strip()
    if cleaned_content.startswith('```json'):
        cleaned_content = cleaned_content[len('```json'):]
    elif cleaned_content.startswith('```'):
        cleaned_content = cleaned_content[len('```'):]
    if cleaned_content.endswith('```'):
        cleaned_content = cleaned_content[:-len('```')]
    cleaned_content = cleaned_content.strip()

    try:
        parsed_data = json.loads(cleaned_content)
    except json.JSONDecodeError as e:
        print(f"Error: OpenAI response content is not valid JSON AFTER CLEANING: {cleaned_content}")
        raise ValueError(f"OpenAI did not return valid JSON after cleaning: {e}")

    return json.dumps(parsed_data)

def extract_rating_scales(clinical_note: str) -> str:
    """
    Extracts any rating scale related data (e.g., PHQ-9, GAD-7, etc.) from a clinical note using OpenAI.
    Returns a JSON string with a 'rating_scales' key mapping scale names to answers.
    """
    if not subscription_key:
        raise ValueError("OpenAI API key not set.")

    system_prompt = """
    You are a medical data extraction assistant. Your task is to identify and extract any **rating scale data** mentioned in a clinical note.

    ---

    ## Definition

    A **rating scale** in this context refers to a standardized clinical questionnaire or assessment tool used to measure symptoms, functioning, or mental health status — for example: PHQ-9, GAD-7, HAM-D, YMRS, BDI, PANSS, etc.

    ---

    ## Extraction Rules

    1. **Identify Rating Scales**
    - Look for explicit mentions of rating scales by name.
    - Only extract if the scale name is clearly stated (e.g., "PHQ-9", "GAD-7").
    - Ignore non-standard self-reported scores or informal checklists.

    2. **Extract Scale Data**
    - Each extracted scale must include:
        - `"name"`: The exact scale name as found in the note.
        - `"answers"`: A dictionary mapping each scale item/question to its reported answer or score.
    - Scores can be numeric (e.g., `3`) or text if not numeric in the note (e.g., `"often"`).

    3. **Multiple Scales**
    - If more than one rating scale is present, list each as a separate object in `"rating_scales"`.

    4. **No Rating Scales Found**
    - If no rating scales are present in the note, return:
        ```json
        { "rating_scales": [] }
        ```

    5. **Exactness**
    - Do not add scales or questions that are not explicitly mentioned.
    - Do not infer missing answers.
    - Use the exact wording of questions/items from the note, without paraphrasing.

    ---

    ## Output Schema

    Return a JSON object in the following format:

    ```json
    {
    "rating_scales": [
        {
        "name": "<string – exact scale name>",
        "answers": {
            "<exact question text>": <numeric or string answer>,
            "...": "..."
        },
        "total_score": <numeric or string total score>
        }
    ]
    } """


    user_prompt = (
        f"Extract any rating scale related data from the following clinical note. "
        f"Return a JSON object with a 'rating_scales' key mapping scale names to answers.\n\n"
        f"### Clinical Note:\n{clinical_note}"
    )

    openai = AzureOpenAI(
        azure_endpoint=endpoint,
        api_version=api_version,
        api_key=subscription_key,
    )

    response = openai.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        # temperature=0,
    )

    raw_openai_response_content = response.choices[0].message.content
    cleaned_content = raw_openai_response_content.strip()
    if cleaned_content.startswith('```json'):
        cleaned_content = cleaned_content[len('```json'):]
    elif cleaned_content.startswith('```'):
        cleaned_content = cleaned_content[len('```'):]
    if cleaned_content.endswith('```'):
        cleaned_content = cleaned_content[:-len('```')]
    cleaned_content = cleaned_content.strip()

    try:
        parsed_data = json.loads(cleaned_content)
    except json.JSONDecodeError as e:
        print(f"Error: OpenAI response content is not valid JSON AFTER CLEANING: {cleaned_content}")
        raise ValueError(f"OpenAI did not return valid JSON after cleaning: {e}")

    return json.dumps(parsed_data)

@task
def validate_gpt_extractions(validation_input):
    """
    Airflow task: Takes a tuple (extracted_json, redacted_text), splits extracted_json into clinical notes, factors, and rating scales, and validates each part.
    Returns a dict with validation results for each extraction, and includes the redacted_text as source_of_truth.
    """
    extracted_json, redacted_text = validation_input
    print("Redacted text for validation:")
    print("\n".join(redacted_text.splitlines()[:5]))  
    try:
        data = json.loads(extracted_json)
    except Exception as e:
        raise ValueError(f"Input to validation is not valid JSON: {e}")

    # Clinical notes: everything except factors and rating_scales
    clinical_notes_data = data.copy()
    factors_data = {}
    rating_scales_data = {}

    # Extract factors (by name) and rating_scales
    for k in list(clinical_notes_data.keys()):
        if k in [
            "Anhedonia", "Depressed Mood", "Fatigue / Low Energy",
            "Appetite / Weight Change", "Self-worth / Guilt", "Concentration Difficulties",
            "Psychomotor Changes", "Psychiatric Co-Morbidities", "Medication Side Effects",
            "Presenting Problem", "Alcohol Consumption", "Appearance & Behaviour",
            "Speech and Language", "Thought Process", "Cognition", "Insight and Judgment",
            "Suicidal Ideation", "Homicidal Ideation", "Life Events / Stressors",
            "Lifestyle / Substance Abuse History", "Sleep/Activity Data (Patient Reported)",
            "Medication History", "Patient's Family History", "Hospitalization"
        ]:
            val = clinical_notes_data[k]
            if isinstance(val, dict) and val.get("isPresent") is True:
                factors_data[k] = clinical_notes_data.pop(k)
            else:
                clinical_notes_data.pop(k)
        elif k == "rating_scales":
            rating_scales_data[k] = clinical_notes_data.pop(k)

    # Prepare JSON strings for validation
    clinical_notes_json = json.dumps(clinical_notes_data)
    factors_json = json.dumps(factors_data)
    rating_scales_json = json.dumps(rating_scales_data)

    clinical_notes_result = validate_with_prompt(clinical_notes_json, OPENAI_CLINICAL_NOTES_VALIDATION_PROMPT, redacted_text)
    factors_result = validate_with_prompt(factors_json, OPENAI_FACTORS_VALIDATION_PROMPT, redacted_text)
    if rating_scales_json:
        rating_scales_result = validate_with_prompt(rating_scales_json, OPENAI_RATING_SCALES_VALIDATION_PROMPT, redacted_text)

    failed = []
    for name, result in [
        ("clinical_notes", clinical_notes_result),
        ("factors", factors_result),
        ("rating_scales", rating_scales_result)
    ]:
        print(f"Validating {name} with OpenAI...")
        is_valid = False
        suggested_fix = None
        if isinstance(result, str):
            try:
                cleaned = result.strip('`').strip()
                parsed = json.loads(cleaned)
                is_valid = parsed.get("is_valid", False)
                suggested_fix = parsed.get("suggested_fix")
            except Exception:
                print(f"Could not parse validation result for {name}: {result}")
        elif isinstance(result, dict):
            is_valid = result.get("is_valid", False)
            suggested_fix = result.get("suggested_fix")
        if not is_valid:
            failed.append(name)
            if suggested_fix:
                print(f"Suggested fix for {name}: {suggested_fix}")
                print(f"Errors for {name}: {result.get('errors', [])}")

    if len(failed) > 2:
        raise AirflowFailException(f"Validation failed for: {', '.join(failed)}. DAG stopped.")

    return {
        "clinical_notes_validation": clinical_notes_result,
        "factors_validation": factors_result,
        "rating_scales_validation": rating_scales_result,
    }

def validate_with_prompt(extracted_json, prompt, redacted_text=None):
    openai = AzureOpenAI(
        azure_endpoint=endpoint,
        api_version=api_version,
        api_key=subscription_key,
    )
    if redacted_text is not None:
        prompt = (
            f"You are validating extracted JSON from a clinical note. "
            f"Below is the actual clinical note (source of truth) from which the JSON was extracted. "
            f"Use this as the only source of truth for validation.\n\n"
            f"---\nCLINICAL NOTE (SOURCE OF TRUTH):\n{redacted_text}\n---\n\n"
            f"{prompt}"
        )
    response = openai.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": extracted_json},
        ],
        # temperature=0,
    )
    raw_content = response.choices[0].message.content
    cleaned_content = raw_content.strip()
    if cleaned_content.startswith('```json'):
        cleaned_content = cleaned_content[len('```json'):]
    elif cleaned_content.startswith('```'):
        cleaned_content = cleaned_content[len('```'):]
    if cleaned_content.endswith('```'):
        cleaned_content = cleaned_content[:-len('```')]
    cleaned_content = cleaned_content.strip()
    try:
        parsed_data = json.loads(cleaned_content)
    except Exception:
        parsed_data = cleaned_content
    return parsed_data
