import os
import labelbox as lb
import labelbox.types as lb_types
from labelbox import Client, OntologyBuilder
from labelbox.data.annotation_types import (
    TextData,
    TextEntity,
    LabelList,
    ImageData,
    Rectangle,
    ObjectAnnotation,
    Label
)
from labelbox.data.serialization import NDJsonConverter
from labelbox.schema.data_row_metadata import DataRowMetadataKind
import tqdm
import uuid
from uuid import uuid4 ## to generate unique IDs
import datetime, csv, re
from bs4 import BeautifulSoup, NavigableString


from collections import defaultdict


def is_html(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        contents = file.read()

    try:
        BeautifulSoup(contents, 'html.parser')
        return True
    except Exception as e:
        print(f"File could not be parsed as HTML: {e}")
        return False

def read_csv(file_name):
    data=[]
    with open(file_name, 'r') as file:
        reader = csv.reader(file, delimiter=',')
        header = next(reader)
        data = [row for row in reader]
    return data

def get_previous_sentences(tag, count=2):
    sentences = []
    text = ''
    for item in reversed(tag.find_all_previous(string=True)):
        if isinstance(item, NavigableString):
            text = item.string.strip() + ' ' + text
            sentences = re.split(r'(?<=[.!?])\s+', text.strip())
            if len(sentences) >= count:
                break
    return ' '.join(sentences[-count:])


def extract_url_paragraph(html_content, target_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    paragraph = None

    target_url = target_url.strip("'")

    for a_tag in soup.find_all('a', href=True):
        url = a_tag['href']
        if url == target_url:  # Check if it is the target URL
            paragraph = get_previous_sentences(a_tag, count=2)
            if paragraph:
                break  # Exit the loop after finding the target URL and paragraph

    return paragraph


def get_clean_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    for script in soup(["script", "style"]): 
        script.extract()  # Remove these two types of tags
    text = soup.get_text()  # Get the text
    lines = (line.strip() for line in text.splitlines())  # break into lines and remove leading and trailing space on each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))  # break multi-headlines into a line each
    text = '\n'.join(chunk for chunk in chunks if chunk)  # Get rid of all blank lines and ends of lines
    return text

def find_a_tag_with_url(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    for a_tag in soup.find_all('a'):
        if a_tag.get('href') == url:
            return a_tag.get_text()  # return the anchor text of the a_tag
    return None  # if no a_tag with the given URL is found

def find_position_of_anchor_text_in_text(text, anchor_text):
    start_position = text.find(anchor_text)
    if start_position == -1:
        return -1,0
    else:
        end_position = start_position + len(anchor_text)
        return start_position, end_position

def create_ner_objects(class_name, st, en):
  named_enity = TextEntity(start=st,end=en)
  named_enity_annotation = ObjectAnnotation(value=named_enity, name=class_name)
  return named_enity_annotation

def generate_annotations(datarow,html,clean_text,count,puburls):
    external_id = datarow['external_id']
    uid = datarow['id']

    annotations = []

    count = 0
    for url in puburls:
        anchor = find_a_tag_with_url(html,url)
        start, end = find_position_of_anchor_text_in_text(clean_text, anchor)
        if start == -1:
            continue
        annotate = None
        if count == 0:
            annotate = create_ner_objects("url1", start, end)
        elif count == 1:
            annotate = create_ner_objects("url2", start, end)
        else:
            annotate = create_ner_objects("url3", start, end)
        annotations.append(annotate)
        count += 1

    text = TextData(uid=uid)
    return text, annotations

def get_metadata(puburls, count, category):
    metadata_fields = []
    tag_schema = metadata_ontology.get_by_name("category")
    metadata_fields.append({"name": tag_schema.name, "value": category})

    if count > 1: # more than one published url in news
        if count == 2:
            tag_schema = metadata_ontology.get_by_name("url1")
            metadata_fields.append({"name": tag_schema.name, "value": puburls[0]})
            tag_schema = metadata_ontology.get_by_name("url2")
            metadata_fields.append({"name": tag_schema.name, "value": puburls[1]})
        else:
            # it is 3
            tag_schema = metadata_ontology.get_by_name("url1")
            metadata_fields.append({"name": tag_schema.name, "value": puburls[0]})
            tag_schema = metadata_ontology.get_by_name("url2")
            metadata_fields.append({"name": tag_schema.name, "value": puburls[1]})
            tag_schema = metadata_ontology.get_by_name("url3")
            metadata_fields.append({"name": tag_schema.name, "value": puburls[2]})
    else:
        # it is 1
        tag_schema = metadata_ontology.get_by_name("url1")
        metadata_fields.append({"name": tag_schema.name, "value": puburls[0]})
    return metadata_fields

def createTask(dataset, clean_text, scid, metadata):
    task = dataset.create_data_rows([{
        lb.DataRow.row_data: clean_text,
        lb.DataRow.external_id: scid,
        lb.DataRow.metadata_fields: metadata,
    }])
    task.wait_till_done()
    if task.errors:
        print(task.errors)
    return task


key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJjbGhodTJxZm8wM2lpMDcwMTVjNjAzc2NpIiwib3JnYW5pemF0aW9uSWQiOiJjbGhodTJxZjUwM2loMDcwMWFpNHQwMHNpIiwiYXBpS2V5SWQiOiJjbGh4NDc0aDIwN3NvMDcweGVhb2pmeWZ3Iiwic2VjcmV0IjoiMzlmNGU2YzUwNzg3ODEyOTcwYTdmMjRkMjQ2YWEwZDgiLCJpYXQiOjE2ODQ2NTUxODAsImV4cCI6MjMxNTgwNzE4MH0.IHs74mHg93IGDciZyvtmsXQAe06lcsf8LAbv6gFxtsI"
client = Client(api_key=key)
dataset = client.create_dataset(name="4nn0t3d t3xt")
ontology = client.get_ontology("cli0dfzgr04v907tu8pl7aa8d")

# get existing project
project = client.get_project("cli1iw33x01a607208zrx683j")
#project = client.create_project(name = "EMNLP", media_type=lb.MediaType.Text)
#project.setup_editor(ontology)
ontology_from_project = lb.OntologyBuilder.from_project(project)
metadata_ontology = client.get_data_row_metadata_ontology()
#project.update(queue_mode=project.QueueMode.Batch)

# get existing dataset
#dataset = client.get_dataset("cli23jdtb0ddo072c1dc717ap")

data1 = read_csv('../../data/14-18.csv')
data2 = read_csv('../../data/19-22.csv')
data = data1 + data2

filtered_data = read_csv('./newurldata.csv')

print(f"init & preprocess & read all files done\nlength of data:{len(filtered_data)}")
current_index = 0
belo = 0
for row in data:
    if belo <= 3981:
        belo+=1
        continue
    sciurl = row[5]
    scid = row[0]
    category = row[3]
    puburls = []
    exist = False

    id_counts = {}
    for paper in filtered_data:
        if scid == paper[0]:
            puburls.append(paper[2])
            exist = True
            id_counts[scid] = id_counts.get(scid, 0) + 1  # Increment count for this ID
    if exist:
        try:
            html = row[7]
            count = sum(id_counts.values())
            clean_text = get_clean_text(html)

            metadata = get_metadata(puburls, count, category)
            task = createTask(dataset,clean_text, str(scid), metadata)

            batch_datarows = []
            for item in task.result:
                batch_datarows.append(item['id'])

            batch = project.create_batch(
                str(current_index) + "_" + str(uuid4()), # name of the batch
                batch_datarows, # list of Data Rows
                1 # priority between 1-5
            )
            
            ground_truth_list = LabelList()
            for item in tqdm.tqdm(task.result):
                result = generate_annotations(item,html,clean_text,count,puburls)
                ground_truth_list.append(Label(
                      data = result[0],
                      annotations = result[1]
                ))

            ## Convert model predictions to NDJSON format
            ground_truth_list.assign_feature_schema_ids(OntologyBuilder.from_project(project))
            ground_truth_ndjson = list(NDJsonConverter.serialize(ground_truth_list))

            ## Upload model predictions as ground truth
            upload_task = lb.LabelImport.create_from_objects(client, project.uid, f"upload-job-{uuid4()}", ground_truth_ndjson)
            upload_task.wait_until_done()
            if upload_task.errors:
                print(upload_task.errors)
            else:
                print(f"ID {scid} is annotated {count} times")
        except Exception as e:
            print(f"Couldn't do ID {scid} because: \n\n{e}")
        #print(str((time.time() - start_time))+" seconds")

# datarows is just a list of dictionaries with attributes:
# the actual text and the external id


