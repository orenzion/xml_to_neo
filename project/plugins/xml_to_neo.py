import os

from utils.neo4j import Neo4J
from xml.etree import ElementTree


def parse_xml_file(xml_file_path: str, neo4j_uri: str, username: str, password: str):
    neo = Neo4J(uri=neo4j_uri, user=username, password=password)
    print(os.getcwd())
    with open(xml_file_path, 'r') as f:
        xml_data = f.read()

    tree = ElementTree.fromstring(xml_data)

    file_name_with_extension = xml_file_path.split('/')[-1]
    file_name = file_name_with_extension.split('.')[0]

    for entry in tree.findall('{http://uniprot.org/uniprot}entry'):
        neo.save_uniprot_entry(entry=entry, protein_id=file_name)

    neo.close()


if __name__ == '__main__':
    parse_xml_file(xml_file_path='./data/Q9Y261.xml',
                   neo4j_uri='neo4j://localhost:7687',
                   username='neo4j',
                   password='password')
