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