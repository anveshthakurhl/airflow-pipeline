from domain_mappers.DomainMapper import DomainMapper
from typing import List, Dict, Any

class VDMapper(DomainMapper):
    def map(self, notes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        all_rows = []
        for note in notes:
            usubjid = note.get("subject_id") or note.get("subjid")
            provider_note_timestamp = note.get("provider_note_timestamp", "")
            if not provider_note_timestamp:
                from datetime import datetime
                provider_note_timestamp = datetime.now().isoformat()

            print(f"mapping note: {note}")

            vitals_json = note.get("json", {})
            vitals_map = [
                ("BP_SYS", "Systolic Blood Pressure", vitals_json.get("patient_vitals_bp", {}).get("systolic"), vitals_json.get("patient_vitals_bp", {}).get("unit")),
                ("BP_DIA", "Diastolic Blood Pressure", vitals_json.get("patient_vitals_bp", {}).get("diastolic"), vitals_json.get("patient_vitals_bp", {}).get("unit")),
                ("PULSE", "Pulse", vitals_json.get("patient_vitals_pulse", {}).get("value"), vitals_json.get("patient_vitals_pulse", {}).get("unit")),
                ("RESP", "Respiration Rate", vitals_json.get("patient_vitals_respiration", {}).get("value"), vitals_json.get("patient_vitals_respiration", {}).get("unit")),
                ("TEMP", "Temperature", vitals_json.get("patient_vitals_temperature", {}).get("value"), vitals_json.get("patient_vitals_temperature", {}).get("unit")),
                ("HEIGHT", "Height", vitals_json.get("patient_vitals_height", {}).get("value"), vitals_json.get("patient_vitals_height", {}).get("unit")),
                ("WEIGHT", "Weight", vitals_json.get("patient_vitals_weight", {}).get("value"), vitals_json.get("patient_vitals_weight", {}).get("unit")),
            ]

            seq = 1
            for code, label, value, unit in vitals_map:
                print(f"Processing vital: {code}, value: {value}, unit: {unit}")
                if value in (None, ""):
                    continue
                try:
                    value_num = float(value)
                except (TypeError, ValueError):
                    value_num = None
                row = {
                    "DOMAIN": "VS",
                    "USUBJID": usubjid,
                    "VSSEQ": seq,
                    "VSTESTCD": code,
                    "VSTEST": label,
                    "VSORRES": value,
                    "VSORRESU": unit,
                    "VSSTRESC": str(value),
                    "VSSTRESN": value_num,
                    "VSSTRESU": unit,
                    "VSBLFL": "",
                    "VISIT": "SCREENING",
                    "VISITNUM": 0,
                    "VSDTC": provider_note_timestamp
                }
                all_rows.append(row)
                seq += 1
        return all_rows
