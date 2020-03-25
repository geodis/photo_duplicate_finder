import os 
import hashlib as hl
import pathlib
import etcd
import sqlite3
import threading

def hash_of(file):
  """
    Hashea el file
  """

  return hl.sha256(open(file,'rb').read()).hexdigest()

def size_of(file):
  """
    Obtiene el size del archivo
  """
  return os.stat(file).st_size

def store_sqlite(info_files):

  """
    Persiste en sqlite la info de los archivos
  """
  DB = "fotos.db" 
  client = None

  def create_db():
    conn = sqlite3.connect(DB)
    fotos_table = '''
      CREATE TABLE FOTOS
        (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        absolute_path TEXT NOT NULL,
        name TEXT NOT NULL,
        hash TEXT NOT NULL,
        size TEXT NOT NULL);'''

    conn.execute(fotos_table)
    print("creo la tabla")
    conn.close()

  def connect():

    if not os.path.isfile(DB):
      create_db()

    return sqlite3.connect(DB)
 
  def insert():
    insert = """
      insert into fotos (absolute_path,name, hash, size)
      values (?, ?, ?, ?)"""

    for info_file in info_files:
      print("insert")
      client.execute(insert, tuple(info_file.values()))
    client.commit()

  client = connect()
  insert()
  
def hash_file(filename, base_path):

  file_data = {}
  file_absolute_path = base_path + "/" + filename 

  file_data['aboslute_path'] = file_absolute_path
  file_data['filename'] = filename
  file_data['hash'] = str(hash_of(file_absolute_path))
  file_data['size'] = str(size_of(file_absolute_path))

  return file_data

def search_in(path):

  """
    Dado path busco los *jpg *JPG y extraigo la info
  """
  info_files = []

  for base_path, _, files in os.walk(path):
    
    for filename in files:
      if any( x in filename for x in [".jpg", ".JPG"]):
        # hasheo solo los jpg
        info_files.append(hash_file(filename, base_path))

  return info_files

"""
def store_etcd(hash, size, filename, base_path, client):
  base = '/fotos'
  key = "/".join([base,hash,size,filename,""])
  print(key)
  client.write(key, base_path)
"""

def main():
  
  # etcd_client = etcd.Client(host='127.0.0.1', port=4001)

  scan_paths = ["/home/chris/src/python/photo_duplicate_finder/ejemplos"]
  files = []

  for base_path in scan_paths:

    info_files = []
    # Busco fotos en los paths
    # TODO: multithreading
    info_files = search_in(base_path)
    # Guardo la info de los files en la BD
    store_sqlite(info_files)

    #  

if __name__ == '__main__':
  main()

