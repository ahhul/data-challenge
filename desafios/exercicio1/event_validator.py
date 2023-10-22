import json
import boto3

_SQS_CLIENT = None

def load_schema(path):
    '''
     Responsável pela leitura do schema
    :param path: Caminho para arquivo json (str)
    :return: dict
    '''

    with open(path, 'r') as file:
        schema_str = file.read()
    return json.loads(schema_str)

def validate_field_type(field_type, value):
    '''
    # Função que compara um tipo do Python e um objeto 
    :param field_type: Tipo do campo no schema (type)
    :param value: Valor do campo no evento (int, str, bool, object)
    :return: bool
    '''

    match field_type:
        case 'integer':
            return isinstance(value, int)
        case 'string':
            return isinstance(value, str)
        case 'boolean':
            return isinstance(value, bool)
        case 'object':
            return isinstance(value, object)
        case _:
            return True


def validate_event(event, schema):
    '''
    # Valida se todos os campos do evento estão cadastrados 
     no schema e se correspondem às definições dos schema
    :param event: Evento  (dict)
    :param schema: Schema (dict)
    :return: bool
    '''

    for field in event:
        if field not in schema["properties"].keys():
            return False
    for field, value in schema["properties"].items():
        if field not in event or not validate_field_type(value["type"], event[field]):
            return false
        # print(field)
        if isinstance(event[field], dict):
            return validate_event(event[field], schema["properties"][field])
    return True

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response["QueueUrl"]
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    '''
     Responsável pela checagem e preparação para o envio do evento
    :param event: Evento  (dict)
    :return: None
    '''

    schema = load_schema("schema.json")
    if validate_event(event, schema):
        send_event_to_queue(event, "valid-events-queue")
    else:
        print("Error: event does not match the schema")
