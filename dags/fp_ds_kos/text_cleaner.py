import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\' ]', '', str(text)) if text else None