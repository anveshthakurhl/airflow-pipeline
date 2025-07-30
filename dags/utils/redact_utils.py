import os
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from airflow.decorators import task

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

@task
def redact_text(file_path: str) -> str:
    """
    Redacts sensitive information from clinical notes.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "r") as f:
        file_content = f.read()
    
    if not file_content:
        raise ValueError("File content is empty.")

    results = analyzer.analyze(
        text=file_content, entities=["PHONE_NUMBER", "PERSON", "EMAIL_ADDRESS"], language="en"
    )
    anonymized_text = anonymizer.anonymize(text=file_content, analyzer_results=results)

    return anonymized_text.text
