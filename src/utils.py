import re


# TODO add test for this
def match_date(date: str) -> str:
    """This functions looks for a date with the format 2022-01-01 in a string"""
    match = re.search(r"\d+-\d+-\d+", date)
    try:
        return match.group(0)
    except AttributeError:
        raise AttributeError(f"No match found for string {date}")
    # TODO: check if remove this unexpected scenario
    except Exception as e:  # unexpected error
        raise e


def convert_int(data: str) -> int:
    """transform a string with the format 999.999.999 to an int 999999999"""
    # TODO add tests and exceptions for this function
    return int(data.replace('.', ''))


def to_100(data: str) -> float:
    """
    For processing imacec columns on central bank dataset:
    Data science findings: "mirando datos del bc, pib existe entre ~85-120 - igual esto es cm (?)"
    """
    # TODO: include tests for this function and exception handling
    data = data.split('.')
    if data[0].startswith('1'):  # es 100+
        if len(data[0]) > 2:
            return float(data[0] + '.' + data[1])
        else:
            data = data[0] + data[1]
            return float(data[0:3] + '.' + data[3:])
    else:
        if len(data[0]) > 2:
            return float(data[0][0:2] + '.' + data[0][-1])
        else:
            data = data[0] + data[1]
            return float(data[0:2] + '.' + data[2:])
