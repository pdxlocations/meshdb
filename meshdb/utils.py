def decimal_to_hex(decimal_number):
    return f"!{decimal_number:08x}"


def hex_to_decimal(hex_string: str) -> int:
    """Convert a Meshtastic-style hex string like '!deadbeef' back to an integer."""
    if hex_string.startswith("!"):
        hex_string = hex_string[1:]
    return int(hex_string, 16)


def convert_to_camel_case(string):
    words = string.split("_")
    camel_case_string = "".join(word.capitalize() for word in words)
    return camel_case_string
