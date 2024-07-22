import requests 
import time 
import json


reports_dict = {
    "SMS Daily AR Offer Alert Report": 7044661597,
    "SMS Redrop/Recommendations Report": 7044671905,
    "SMS Upcoming Schedule": 7044687692,
    "Swap Report": 7044689311,
    "SMS Team Daily Alert Report": 7044804654,
    "SMS Content Request Report": 6954763228,
    "SMS Jump Page Feedback Report": 6954763233,
    "SMS Content Feedback Loop Report": 6954763241,
    "SMS Team Offer Alert Report": 6954763249,
    "SMS Budget Report": 6954763244
}


def add_file_monday (file_path, filename, item_id): # change file 
    
        # Replace with your Monday.com API token
    apiKey = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjM4NTgwOTE2NiwiYWFpIjoxMSwidWlkIjoxNjQ3NDk4MywiaWFkIjoiMjAyNC0wNy0xN1QyMjozMToxMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjEwMDE4MywicmduIjoidXNlMSJ9.3TRaKq9JxxJOpzFYeq6_-x3NSvwuBQoYH8Ws6Npscg0'
    headers = {"Authorization" : apiKey, 'API-version':'2024-07'}
    url = "https://api.monday.com/v2/file"
    # add file to column
    query = 'mutation add_file($file: File!) {add_file_to_column (item_id: %d, column_id:"file__1" file: $file) {id}}' % (item_id)
    payload = {
        'query':query,
        'map': '{"file":"variables.file"}'
        }
    
    files = [
        ('file', (filename, open(file_path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
    ]

    # files = [(<key>, (<filename>, open(<file location>, 'rb'), <content type>))]

    response = requests.post(url, headers=headers, data=payload, files=files)
    print(response.text)
    time.sleep(30)



def clean_file(item_id):
    
    # Replace with your Monday.com API token
    apiKey = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjM4NTgwOTE2NiwiYWFpIjoxMSwidWlkIjoxNjQ3NDk4MywiaWFkIjoiMjAyNC0wNy0xN1QyMjozMToxMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjEwMDE4MywicmduIjoidXNlMSJ9.3TRaKq9JxxJOpzFYeq6_-x3NSvwuBQoYH8Ws6Npscg0'
    headers = {
        "Authorization": apiKey,
        "Content-Type": "application/json"
    }
    url = "https://api.monday.com/v2"

    # Step 1: Clear the existing files in the column
    query_clear = """
    mutation {
        change_column_value(
            board_id: 6954763157,
            item_id: %d,
            column_id: "file__1",
            value: "{\\"clear_all\\": true}"
        ) {
            id
        }
    }
    """ % item_id

    payload_clear = {'query': query_clear}
    response_clear = requests.post(url, headers=headers, json=payload_clear)
    print("Clear response:", response_clear.text)
    time.sleep(30)



def upload_file(file_path, filename, item_id):
    clean_file(item_id)
    add_file_monday(file_path, filename, item_id)
    


def add_update_to_item(item_id, update_text, type = None):
    time. sleep(30)
    if type == "HTML":
        update_text =  update_text.replace('"', '\\"')
    apiKey = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjM4NTgwOTE2NiwiYWFpIjoxMSwidWlkIjoxNjQ3NDk4MywiaWFkIjoiMjAyNC0wNy0xN1QyMjozMToxMS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjEwMDE4MywicmduIjoidXNlMSJ9.3TRaKq9JxxJOpzFYeq6_-x3NSvwuBQoYH8Ws6Npscg0'

    headers = {
        "Authorization": apiKey,
        "Content-Type": "application/json"
    }
    url = "https://api.monday.com/v2"
    
    query = """
    mutation {
        create_update(
            item_id: %d,
            body: "%s"
        ) {
            id
        }
    }
    """ % (item_id, update_text)
    
    payload = {
        'query': query
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    print(response.text)
    