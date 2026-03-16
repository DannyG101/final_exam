from datetime import datetime
from typing import Optional

from pydantic import BaseModel

class Report(BaseModel):

    timestamp : datetime
    attack_id : str
    entity_id : str
    weapon_type : str


class Validator:

    @staticmethod
    def validate_report(report):
        try:
            report_check = Report(**report)
            return {"status": True, "report": report_check.model_dump()}
        except Exception as e:
            return {"status": False, "report": report, "error": str(e)}



