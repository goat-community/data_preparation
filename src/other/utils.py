import os
import shutil
from rich import print as print
from urllib.request import urlopen, Request
from pathlib import Path

def delete_file(file_path: str) -> None:
    """Delete file from disk."""
    try:
        os.remove(file_path)
    except OSError as e:
        pass

def delete_dir(dir_path: str) -> None:
    """Delete file from disk."""
    try:
        shutil.rmtree(dir_path)
    except OSError as e:
        pass

def print_hashtags():
    print("#################################################################################################################")

def print_info(message: str):
    print(f"INFO: {message}")

def print_warning(message: str):
    print(f"WARNING: {message}")

def download_link(directory, link):
    download_path = Path(directory) / os.path.basename(link)
    with urlopen(link) as image, download_path.open('wb') as f:
        f.write(image.read())
    print_info(f'Downloaded ended for {link}')
