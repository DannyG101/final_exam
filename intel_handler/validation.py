from datetime import datetime
from typing import Optional

from pydantic import BaseModel

class Report(BaseModel):

    timestamp : datetime
    signal_id : str
    entity_id : str
    reported_lat : float
    reported_lon : float
    signal_type : str
    priority_level : Optional[int] = 99

def validate_report(report):
    try:
        report_check = Report(**report)
        return True, report_check.model_dump()
    except Exception as e:
        return False, str(e)

