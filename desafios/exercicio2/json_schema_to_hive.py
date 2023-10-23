import json
_ATHENA_CLIENT = None

def load_schema(path):
    '''
    Responsável pela leitura do schema
    :param path: Caminho para arquivo json (str)
    :return: dict
    '''

    with open(path, "r") as file:
        schema_str = file.read()
    return json.loads(schema_str)


def generate_attributes(field, value):
    '''
    # Cria string representando termos da query para
    cada campo do schema
    :param field: Nome do campo (str)
    :param value: Propriedades do campo (dict)
    :return: str
    '''

    type_map = {"string" : "VARCHAR (255)", "integer": "INT", "boolean": "BOOLEAN", "object": "STRUCT"}
    sub_query = ""
    if value["type"] == "object":
        for nested_field, nested_value in value["properties"].items():
            sub_query += f"\n\t{nested_field} {type_map[nested_value['type']]} COMMENT '{nested_value['description']}',".expandtabs(8)
        return f"\n\t{field} STRUCT < {sub_query[:-1]}\n\t>".expandtabs(4)
    if value["type"] in type_map.keys():
        sub_query += f"\n\t{field} {type_map[value['type']]} COMMENT '{value['description']}',".expandtabs(4)
        return sub_query

def generate_query(table_metadata):
    '''
    # Função que gera query para criação de tabela HIVE,
    baseada em um dicionário com metadados da tabela
    :param table_metadata: Metadados da tabela (dict)
    :return: str
    '''

    query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_metadata['name']} ("
    for field, value in table_metadata["schema"]["properties"].items():
        query += generate_attributes(field, value)
    if table_metadata["location"]:
        query += f"\nLOCATION '{table_metadata['location']}'"
    if table_metadata["storage_format"]:
        query += f"\nSTORED AS {table_metadata['storage_format']}"
    if table_metadata["partition"]:
        query += f"\nPARTITION BY {table_metadata['partition']}"
    if table_metadata["row_format"]:
        query += f"\nROW FORMAT {table_metadata['row_format']}"
    if table_metadata["properties"]:
        query += f"\nTBLPROPERTIES {table_metadata['properties']}"
    return query

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query:\n{query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": f"s3://iti-query-results/"
        }
    )

def handler():
    '''
    # Função que invoca a criação de query para criação de tabela
    e invoca envio da requisição para criação no Athena
    '''

    schema = load_schema("schema.json")
    table_metadata = {
        "schema": schema,
        "name": "cliente",
        "location": "s3a://bucket_name/path",
        "storage_format":"PARQUET",
        "partition": None,
        "row_format": "SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'",
        "properties": None}
    query = generate_query(table_metadata)
    create_hive_table_with_athena(query)