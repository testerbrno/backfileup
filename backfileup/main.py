# main.py
import os
import csv
from hashlib import sha512
import time
from joblib import Parallel, delayed
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from file_scanner import FileScanner
from database_controller import DatabaseManager

class Preprocessing:
    def __init__(self) -> None:
        self.target_directory = '/media/programovani/Acer/'
        self.scanner = FileScanner()
        self.manager = DatabaseManager()
        self.scanner.scan_for_files(self.target_directory)
    
    def save_dict_to_csv(self, dictionary, csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for key, value in dictionary.items():
                writer.writerow((key, value))

    def process_files_to_database(self):   
        processing_method = 2
        match processing_method:
            case 1:        
                start_time_results = time.time()
                results = dict(Parallel(n_jobs=-1)(map(delayed(self.scanner.create_file_hash), self.scanner.file_paths)))
                end_time_results = time.time()
                print(f"Parallel Total for results time: {end_time_results - start_time_results} seconds")
                
            case 2:
                start_time_results = time.time()
                with Pool() as pool:
                    results = dict(pool.map(self.scanner.create_file_hash, self.scanner.file_paths))
                end_time_results = time.time()
                print(f"Pool Total for results time: {end_time_results - start_time_results} seconds")
                
            case 3:
                start_time_results = time.time()
                with ThreadPoolExecutor() as executor:
                    results = dict(executor.map(self.scanner.create_file_hash, self.scanner.file_paths))
                end_time_results = time.time()
                print(f"Multithreading Total for results time: {end_time_results - start_time_results} seconds")

            # case 4:
            #     start_time_results = time.time()
            #     results = map(self.scanner.create_file_hash, self.scanner.file_paths)
            #     end_time_results = time.time()
            #     print(f"Simple Total for results time: {end_time_results - start_time_results} seconds")
        
        start_time_inserting = time.time()
        # self.save_dict_to_csv(results, "results_file.csv")
        self.manager.insert_file_hashes(results)
        end_time_inserting = time.time()
        print(f"Insert data to database time: {end_time_inserting - start_time_inserting} seconds")

        return results

def main():
    # Měření celkového času běhu programu
    start_time_total = time.time()
    start_time_scanning = time.time()

    # Skenování a vkládání do databáze
    preprocesing_instance = Preprocessing()
    preprocesing_instance.process_files_to_database()

    end_time_scanning = time.time()
    print(f"Scanning and inserting to db {end_time_scanning - start_time_scanning} seconds")

    # Získání a výpis duplikátů
    # start_time_duplicates = time.time()
    # duplicate_file_paths = preprocesing_instance.manager.get_duplicate_file_paths()
    # for paths_string, hash_value in duplicate_file_paths:
    #     print(f"Hash: {hash_value}")
    #     duplicate_paths = paths_string.split(', ')
    #     for path in duplicate_paths:
    #         print(f"  Path: {path}")
    
    # end_time_duplicates = time.time()
    # print(f"Duplicates finding time {end_time_duplicates - start_time_duplicates} seconds")

    end_time_total = time.time()
    print(f"Total time {end_time_total - start_time_total} seconds")

if __name__ == "__main__":
    main()
