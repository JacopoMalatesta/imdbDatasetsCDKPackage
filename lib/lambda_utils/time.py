from datetime import datetime
from typing import Dict


def get_runtime_year_month_day() -> Dict[str, int]:
    run_year_month_day: Dict[str, int] = dict()
    runtime: datetime = datetime.now()
    run_year_month_day["year"] = runtime.year
    run_year_month_day["month"] = runtime.month
    run_year_month_day["day"] = runtime.day
    return run_year_month_day
