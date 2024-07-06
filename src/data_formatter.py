# src/data_formatter.py
import uuid

def format_data(res):
    if not res:
        return None
    data = {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{res['location']['street']['number']} {res['location']['street']['name']}, "
                   f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}",
        'post_code': res['location']['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }
    return data