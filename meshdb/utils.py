def decimal_to_hex(decimal_number):
    return f"!{decimal_number:08x}"


def convert_to_camel_case(string):
    words = string.split("_")
    camel_case_string = "".join(word.capitalize() for word in words)
    return camel_case_string
