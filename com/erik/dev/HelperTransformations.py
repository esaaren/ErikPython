import re


class HelperTransformations:
    @staticmethod
    def removeStringSpecialCharacters(s):
        # Replace special characters with " "
        stripped = re.sub("[^\w\s\-\_]", "", s)
        # Change any whitespace to one space
        stripped = re.sub("\s+", " ", stripped)
        # Remove start and end whitespace
        stripped = stripped.strip()
        return stripped
