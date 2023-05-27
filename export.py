import labelbox as lb
import labelbox.types as lb_types
from labelbox import Client
import json

import ndjson

def extract_text_segments(file_path):
    # Dictionary to hold results
    text_segments = {}

    with open(file_path, 'r') as f:
        data = ndjson.load(f)

        for entry in data:
            # Get the id and text
            id = entry['data_row']['external_id']
            text = entry['data_row']['row_data']

            # Get the start and end positions
            labels = entry['projects'].values()
            for label in labels:
                for l in label['labels']:
                    objects = l['annotations']['objects']
                    for obj in objects:
                        start = obj['location']['start']
                        end = obj['location']['end']

                        # Extract the text segment
                        text_segment = text[start:end]

                        # If id doesn't exist in dictionary, create a list
                        if id not in text_segments:
                            text_segments[id] = []

                        # Append text segment to the id
                        text_segments[id].append(text_segment)

    return text_segments

text = extract_text_segments('./export/export-result.ndjson')

# loop to process the data
for id,article in text.items():
	print(f"id: {id}\nannotated article:{article}\n\n")

