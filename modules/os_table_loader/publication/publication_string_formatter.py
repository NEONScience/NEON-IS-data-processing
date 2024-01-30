
def format_string(s: str, publication_format: str):
    """Format a string value according to the given publication format."""
    if publication_format == 'UPPER':
        return s.upper()
    elif publication_format == 'lower':
        return s.lower()
    elif publication_format == 'Title':
        return s.title()
    else:
        return s
