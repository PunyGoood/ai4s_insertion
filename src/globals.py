# id_generator.py
import threading

id_lock = threading.Lock()
ID_FILE = "id_data.txt"

def load_id():
    try:
        with open(ID_FILE, "r") as f:
            id_data = f.read().strip()
            if id_data.isdigit():
                return f"{int(id_data):06d}"
    except FileNotFoundError:
        return "004041"

def save_id(id_data):
    with open(ID_FILE, "w") as f:
        f.write(id_data)

id_data = load_id()

def increment_id():
    global id_data
    with id_lock:
        current_id = int(id_data)
        new_id = current_id + 1
        id_data = f"{new_id:06d}"
        save_id(id_data)
    return id_data