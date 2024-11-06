import json

def jsonlize(input_file, output_file):
    """
    Reads a JSON file and writes it to a JSON Lines file.
    """
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        # Read the entire content of the file
        content = infile.read()

        # Replace MongoDB extended JSON types if necessary
        content = content.replace('$oid', 'oid').replace('$date', 'date')

        # Split the content into individual JSON objects
        json_objects = content.strip().split('\n')

        for obj_str in json_objects:
            # Parse the JSON object
            try:
                obj = json.loads(obj_str)
                # Write the JSON object as a single line
                outfile.write(json.dumps(obj) + '\n')
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

if __name__ == "__main__":

    jsonlize('users.json', 'users.jsonl')
    jsonlize('receipts.json', 'receipts.jsonl')
    jsonlize('brands.json', 'brands.jsonl')

    print("Conversion to JSON Lines format completed.")


