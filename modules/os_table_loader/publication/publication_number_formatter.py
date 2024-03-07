
def format_number(number, publication_format) -> str:
    """Format a numerical value according to the given publication format."""
    if publication_format == '*.#(round)':
        return str(round(number, 1))
    elif publication_format == 'signif_#(round)':
        return str(round_to_digits(number, 1))
    elif publication_format == '*.##(round)':
        return str(round(number, 2))
    elif publication_format == 'signif_##(round)':
        return str(round_to_digits(number, 2))
    elif publication_format == '*.###(round)':
        return str(round(number, 3))
    elif publication_format == 'signif_###(round)':
        return str(round_to_digits(number, 3))
    elif publication_format == '*.####(round)':
        return str(round(number, 4))
    elif publication_format == 'signif_####(round)':
        return str(round_to_digits(number, 4))
    elif publication_format == '*.#####(round)':
        return str(round(number, 5))
    elif publication_format == 'signif_#####(round)':
        return str(round_to_digits(number, 5))
    elif publication_format == '*.######(round)':
        return str(round(number, 6))
    elif publication_format == '*.#########(round)':
        return str(round(number, 9))
    elif publication_format == 'signif_###########(round)':
        return str(round_to_digits(number, 11))
    elif publication_format == 'integer':
        return str(int(number))
    else:
        return number


def round_to_digits(number: float, significant_digits: int) -> float:
    """
    Round to a specific number of digits starting from the first non-zero value
    (i.e., starting after the last leading 0, if there are any).
    The minimum value (lower bound) is 0.000 + significant digits.
    """
    split_str = str(number).split('.')
    try:
        number_str = split_str[1]
    except IndexError:
        number_str = split_str[0]
    last_zero_index = number_str.rfind('0')
    if last_zero_index == -1:
        return round(number, significant_digits)
    rounded = round(number, last_zero_index + significant_digits + 1)
    return rounded
