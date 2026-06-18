from ..api_client import get


def history(start_date, end_date):

    return get(
        "/v1/timeseries",
        {
            "start": start_date,
            "end": end_date
        }
    )