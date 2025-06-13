def format_phone_number(phone_number: str) -> str:
    """
    Format a phone number into ddd-ddd-dddd format.

    Args:
        phone_number (str): The phone number to format (can include or exclude dashes/spaces)

    Returns:
        str: Formatted phone number in ddd-ddd-dddd format

    Examples:
        >>> format_phone_number("1234567890")
        '123-456-7890'
        >>> format_phone_number("123-456-7890")
        '123-456-7890'
        >>> format_phone_number("123 456 7890")
        '123-456-7890'
    """
    # Remove all non-digit characters
    digits = "".join(filter(str.isdigit, phone_number))

    # Check if we have exactly 10 digits
    if len(digits) != 10:
        raise ValueError("Phone number must contain exactly 10 digits")

    # Format the number
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
