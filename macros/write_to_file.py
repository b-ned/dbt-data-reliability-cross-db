# macros/write_to_file.py

import json

def write_to_file(data, filename):
    with open(filename, 'w') as file:
        file.write(data)