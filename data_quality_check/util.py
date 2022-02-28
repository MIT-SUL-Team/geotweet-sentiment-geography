import pandas as pd
from datetime import timedelta, date

def months_in_between(start, end):
    """
    @param start: datetime object, start date
    @param end: datetime object, end date

    returns: months in between the start and the end as a list (Ex. ["2008_08", "2008_09"])
    """
    delta = end - start  # as timedelta
    months_with_duplicate = [f"{start + timedelta(days=i):%Y_%m}" for i in range(delta.days + 1)]
    months = pd.Series(months_with_duplicate).unique()
    return months

def leap_year(year):
    if year % 400 == 0:
        return True
    if year % 100 == 0:
        return False
    if year % 4 == 0:
        return True
    return False


def days_in_month(month, year):
    if month in {1, 3, 5, 7, 8, 10, 12}:
        return 31
    if month == 2:
        if leap_year(year):
            return 29
        return 28
    return 30


def hours_in_day():
    return [str(num).zfill(2) for num in range(0, 24)]


def get_date_list(year):
    base = date(year, 1, 1)
    numdays = 365 + int(leap_year(year)) * 1
    return [base + timedelta(days=x) for x in range(numdays)]




