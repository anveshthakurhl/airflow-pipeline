import os
import re
from datetime import datetime, timedelta
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine
from airflow.decorators import task

# Initialize analyzer and anonymizer
analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

# Define custom address pattern
address_pattern = Pattern(
    name="custom_address",
    regex=r"\d{1,5}\s+\w+(?:\s\w+)*\s+(Ave|St|Street|Road|Blvd|Lane|Ln|Dr|Way)\b",
    score=0.85,
)

# Create and register custom address recognizer
custom_address_recognizer = PatternRecognizer(
    supported_entity="ADDRESS",
    patterns=[address_pattern]
)
analyzer.registry.add_recognizer(custom_address_recognizer)


def shift_dates_by_days(text: str, days: int = 3) -> str:
    """
    Finds all dates in the format like 'July 24, 2025', '2025-07-24', '07/24/2025'
    and shifts them by the given number of days.
    """

    # Regex for various date formats
    date_patterns = [
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b",  # July 24, 2025
        r"\b\d{4}-\d{2}-\d{2}\b",  # 2025-07-24
        r"\b\d{1,2}/\d{1,2}/\d{4}\b"  # 07/24/2025
    ]

    def replace_date(match):
        date_str = match.group(0)
        formats = ["%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"]
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                new_date = parsed_date + timedelta(days=days)
                return new_date.strftime(fmt)
            except ValueError:
                continue
        return date_str  # return unchanged if parsing fails

    for pattern in date_patterns:
        text = re.sub(pattern, replace_date, text)

    return text


@task
def redact_text(file_path: str) -> str:
    """
    Redacts sensitive information from clinical notes,
    keeps subject IDs (e.g., SUBJ001) and medical titles (e.g., MD) visible,
    detects custom addresses, and shifts dates by +3 days.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r") as f:
        file_content = f.read()

    if not file_content:
        raise ValueError("File content is empty.")

    # Step 1: Shift dates
    file_content = shift_dates_by_days(file_content, days=3)

    # Step 2: Analyze for PII including LOCATION and our custom ADDRESS
    results = analyzer.analyze(
        text=file_content,
        entities=["PHONE_NUMBER", "PERSON", "EMAIL_ADDRESS", "LOCATION", "ADDRESS"],
        language="en",
    )

    # Step 3: Filter false positives
    subj_pattern = re.compile(r"subj[-_ ]?\d+", re.IGNORECASE)  # subject IDs
    two_letter_abbr_pattern = re.compile(r"^[A-Z]{2}$")  # MD, RN, PA

    filtered_results = []
    for r in results:
        detected_text = file_content[r.start:r.end].strip()

        if r.entity_type == "LOCATION" and subj_pattern.fullmatch(detected_text):
            continue
        if r.entity_type == "LOCATION" and two_letter_abbr_pattern.fullmatch(detected_text):
            continue

        filtered_results.append(r)

    # Step 4: Perform anonymization
    anonymized_text = anonymizer.anonymize(
        text=file_content, analyzer_results=filtered_results
    )

    return anonymized_text.text