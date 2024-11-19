import sys, os
import hashlib
import tempfile
import subprocess

def get_hash(file):
  
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
  
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
  
    return sha256.hexdigest()

def is_file_modified(filename, file_path, url):
    # Create a temporary file path with .xlsx suffix which will automatically get deleted once operation is completed
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "temp.xlsx")
        command = f"wget -O {temp_file_path} {url}"
        subprocess.run(command, shell=True, check=True)
        f1=get_hash(temp_file_path)

    f2=get_hash(file_path)

    if f1==f2:
        print(f"File '{filename}' hasn't been modified. Skipping replacing")
        return False
    else:
        print(f"File '{filename}' has been modified")
        return True
        command = f"wget -O '{file_path}' '{url}'"  # wget command to download and save with specific filename
        os.system(command)  # Execute the command
        print(f"Re-Downloaded and saved: {filename} from {url}")

def nepali_to_english_number(nepali_str):
    nepali_num_map = str.maketrans('०१२३४५६७८९', '0123456789')
    return nepali_str.translate(nepali_num_map)

