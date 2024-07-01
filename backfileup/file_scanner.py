
import os
from hashlib import sha512
import time

class FileScanner:
    def __init__(self):
        self.file_hashes = {}
        self.file_paths = []

    def create_file_hash(self, file_path):  # Velikost bloku 64 KB
        block_size=65536
        try:
            with open(file_path, 'rb') as f:
                hash_obj = sha512()  # Vytvoření objektu hash
                while True:
                    block = f.read(block_size)  # Načtení bloku dat
                    if not block:  # Pokud je blok prázdný, je soubor přečten
                        break
                    hash_obj.update(block)  # Aktualizace hashu s aktuálním blokem
                hash_value = hash_obj.hexdigest()  # Získání výsledného hashe
        except Exception as e:
            with open("error_log.txt", "a") as error_file:
                error_file.write(f"Error processing file: {file_path}. Error: {e}\n")
            return None, None
        return file_path, hash_value
    
    def scan_for_files(self, target_directory):
        start_time_file_paths = time.time()
        self.file_paths = []
        
        for root, _, files in os.walk(target_directory):
            self.file_paths.extend(map(lambda file: os.path.join(root, file), files))

        print(len(self.file_paths))
        end_time_file_paths = time.time()
        print(f"Total for file_paths time: {end_time_file_paths - start_time_file_paths} seconds")
        return self.file_paths
