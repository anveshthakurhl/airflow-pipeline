# Prompt for extracting and summarizing clinical findings from notes using OpenAI
OPENAI_CLINICAL_NOTES_PROMPT = """You are a medical data extraction assistant. Given a clinical note, extract the following structured information as JSON.
Use **empty strings ("")** for any missing or unclear values. Keep all field names exactly as specified. `diagnoses` and `medications` should be arrays of objects. Everything else should remain flat.
### Required JSON Format:
```json
{
  "subject_id": "", 
  "patient_chief_complaint": "",
  "patient_subjective_symptoms": "",
  "patient_vitals_temperature": "",
  "patient_vitals_pulse": "",
  "patient_vitals_bp": "",
  "patient_vitals_respiration": "",
  "patient_vitals_weight": "",
  "patient_mental_status": "",

  "provider_name": "",
  "provider_title": "",
  "provider_note_timestamp": "",
  "provider_time_spent": "",

  "diagnoses": [
    {
      "name": "",
      "type": "",
      "coding_system": ""
    }
  ],

  "medications": [
    {
      "name": "",
      "dosage": "",
      "frequency": "",
      "status": ""
    }
  ]
}
"""
