# database_controller.py
import os
import sqlite3
import time

class DatabaseManager:
    def __init__(self, db_name='file_hashes.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.create_database_if_not_exists()

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def create_database_if_not_exists(self):
        if not os.path.exists(self.db_name):
            with self:
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS file_hashes (
                        id INTEGER PRIMARY KEY,
                        hash TEXT UNIQUE
                    )
                ''')
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS file_paths (
                        id INTEGER PRIMARY KEY,
                        path TEXT UNIQUE
                    )
                ''')
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS hash_file_paths (
                        hash_id INTEGER,
                        file_path_id INTEGER,
                        PRIMARY KEY (hash_id, file_path_id),
                        FOREIGN KEY (hash_id) REFERENCES file_hashes(id),
                        FOREIGN KEY (file_path_id) REFERENCES file_paths(id)
                    )
                ''')

                # Add indexes
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_hashes_hash ON file_hashes (hash)')
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_paths_path ON file_paths (path)')
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash_file_paths_hash_id ON hash_file_paths (hash_id)')
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash_file_paths_file_path_id ON hash_file_paths (file_path_id)')

    def execute_query(self, query, values=None):
        if self.conn:
            if values:
                self.cursor.execute(query, values)
            else:
                self.cursor.execute(query)
    
    def insert_hash_into_db(self, file_path, file_hash):
        try:
            self.execute_query('INSERT OR IGNORE INTO file_hashes (hash) VALUES (?)', (file_hash,))
            self.cursor.execute('SELECT id FROM file_hashes WHERE hash = ?', (file_hash,))
            hash_id = self.cursor.fetchone()[0]

            # Přidáno zpracování cesty, aby se získala správná cesta z každého slovníku
            self.execute_query('INSERT OR IGNORE INTO file_paths (path) VALUES (?)', (file_path,))
            self.cursor.execute('SELECT id FROM file_paths WHERE path = ?', (file_path,))
            file_path_id = self.cursor.fetchone()[0]

            self.execute_query('INSERT OR IGNORE INTO hash_file_paths (hash_id, file_path_id) VALUES (?, ?)', (hash_id, file_path_id))
        except Exception as e:
            print(f"Error inserting file paths: {e} \n {file_path} \n {file_hash}")

    def insert_file_hashes(self, file_hashes):
        with self:
            list(map(lambda x: self.insert_hash_into_db(*x), file_hashes.items()))

    def get_file_hashes_from_db(self):
        with self:
            self.execute_query('SELECT file_hashes.hash, file_paths.path FROM file_hashes JOIN hash_file_paths ON file_hashes.id = hash_file_paths.hash_id JOIN file_paths ON hash_file_paths.file_path_id = file_paths.id')
            rows = self.cursor.fetchall()
            return rows

    def get_duplicate_file_paths(self):
        with self:
            start_time = time.time()
            self.execute_query('''
                SELECT file_hashes.hash, GROUP_CONCAT(file_paths.path, ', ')
                FROM file_paths
                INNER JOIN hash_file_paths ON file_paths.id = hash_file_paths.file_path_id
                INNER JOIN file_hashes ON hash_file_paths.hash_id = file_hashes.id
                GROUP BY file_hashes.hash
                HAVING COUNT(file_paths.path) > 1
            ''')
            rows = self.cursor.fetchall()
            end_time = time.time()
            print(f"Fetching duplicates took {end_time - start_time} seconds")
            return rows

    def get_files_by_hash(self, file_hash):
        with self.conn:
            self.cursor.execute('''
                SELECT file_paths.path
                FROM file_paths
                INNER JOIN hash_file_paths ON file_paths.id = hash_file_paths.file_path_id
                INNER JOIN file_hashes ON hash_file_paths.hash_id = file_hashes.id
                WHERE file_hashes.hash = ?
            ''', (file_hash,))
            paths = self.cursor.fetchall()
            return [path[0] for path in paths]
