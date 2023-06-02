import os
import random
import string
import csv
import shutil
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

COUNT_ZIP_FILES = 50
COUNT_XML_FILES = 100
COUNT_TAGS = 10
COUNT_SYMBOLS = 10
TMP_DIR = "/tmp/test_ng/"


# Генерация случайной строки
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


# Создание xml файла со случайными данными
def create_xml_file(file_name):
    root = ET.Element("root")
    var_id = ET.SubElement(root, "var", name="id", value=random_string(10))
    var_level = ET.SubElement(root, "var", name="level", value=str(random.randint(1, 100)))
    objects = ET.SubElement(root, "objects")
    for i in range(random.randint(1, COUNT_TAGS)):
        ET.SubElement(objects, "object", name=random_string(COUNT_SYMBOLS))
    tree = ET.ElementTree(root)
    tree.write(file_name)


# Создание zip архива с xml файлами
def create_zip_archive(zip_name):
    for i in range(COUNT_XML_FILES):
        file_name = f"{i}.xml"
        create_xml_file(file_name)
    os.system(f"zip {TMP_DIR}{zip_name} *.xml")
    os.system(f"rm *.xml")


# Обработка xml файла и запись данных в csv файлы
def process_zip_file(zip_file):
    csv_file_1 = open(f"{TMP_DIR}file_with_level.csv", "a")
    csv_file_2 = open(f"{TMP_DIR}file_with_object_name.csv", "a")
    with open(zip_file, "r") as f:
        zip_name = os.path.splitext(os.path.basename(zip_file))[0]
        zip_dir = f"{TMP_DIR}{zip_name}"
        os.makedirs(zip_dir, exist_ok=True)
        os.system(f"unzip -q {zip_file} -d {zip_dir}")
        for xml_file in os.listdir(zip_dir):
            xml_path = os.path.join(zip_dir, xml_file)
            tree = ET.parse(xml_path)
            root = tree.getroot()
            var_id = root.find(".//var[@name='id']")
            var_level = root.find(".//var[@name='level']")
            objects = root.findall(".//object")
            csv_file_1.write(f"{var_id.get('value')},{var_level.get('value')}\n")
            for obj in objects:
                csv_file_2.write(f"{var_id.get('value')},{obj.get('name')}\n")
        shutil.rmtree(zip_dir)
    csv_file_1.close()
    csv_file_2.close()


def main():
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)

    os.mkdir(TMP_DIR)

    for i in range(COUNT_ZIP_FILES):
        zip_name = f"{i}.zip"
        create_zip_archive(zip_name)

    zip_files = [f"{TMP_DIR}{i}.zip" for i in range(COUNT_ZIP_FILES)]
    with ThreadPool() as p:
        p.map(process_zip_file, zip_files)

if __name__ == "__main__":
    main()
