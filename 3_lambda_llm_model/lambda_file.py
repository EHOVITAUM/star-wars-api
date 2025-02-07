import json
import boto3
import random
from langchain_openai import ChatOpenAI

# Nome da tabela do DynamoDB
TABLE_NAME = "star_wars_characters"

# Função para recuperar a chave da OpenAI do Secrets Manager
def get_secret():
    try:
        secrets_openai_client = boto3.client("secretsmanager")
        get_secret_value_response = secrets_openai_client.get_secret_value(SecretId="OpenAI_API_KEY")
        openai_api_key = json.loads(get_secret_value_response["SecretString"])["openai_key"]
        print("Chave secreta recuperada com sucesso.")
        return openai_api_key
    except Exception as e:
        print(f"Erro ao recuperar chave secreta: {e}")
        raise

# Instancia o modelo LLM
def get_llm_model(api_key):
    try:
        llm = ChatOpenAI(model="gpt-4o-mini-2024-07-18", api_key=api_key)
        print("Modelo LLM instanciado com sucesso.")
        return llm
    except Exception as e:
        print(f"Erro ao instanciar o modelo LLM: {e}")
        raise

# Busca informações do personagem no DynamoDB
def buscar_personagem_dynamodb(character_name):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(TABLE_NAME)

    try:
        response = table.get_item(Key={"characters_name": character_name})
        if "Item" in response:
            print(f"Registro encontrado para {character_name} no DynamoDB.")
        else:
            print(f"Nenhum registro encontrado para {character_name} no DynamoDB.")
        return response.get("Item", None)
    except Exception as e:
        print(f"Erro ao consultar o DynamoDB: {e}")
        raise

# Gera uma história baseada nas informações do personagem
def gerar_historia(llm, personagem, dados_personagem):
    lista_personagens = dados_personagem.get("characters_met", "").split(", ")
    lista_enredos = [
        "uma jornada de redenção",
        "um duelo épico",
        "uma missão secreta",
        "uma guerra entre facções",
    ]

    personagens_escolhidos = random.sample(lista_personagens, min(3, len(lista_personagens)))
    enredo_escolhido = random.choice(lista_enredos)

    prompt = f"""
    Você é um contador de histórias de Star Wars. O usuário quer uma nova história.
    Personagem principal: {personagem}.
    Personagens secundários: {', '.join(personagens_escolhidos)}.
    Enredo: {enredo_escolhido}.
    Crie uma história envolvente baseada nesses elementos.
    """

    print("Gerando a história com o modelo LLM...")
    historia = ""
    for i in llm.stream(prompt):
        historia += i.content

    return historia

# Função Lambda Handler
def lambda_handler(event, context):

    
    try:
        print("Evento recebido:", event)

        # Converte o corpo da requisição para JSON
        # body = json.loads(event.get("body", "{}"))
        data_sns = json.loads(event['Records'][0]['Sns']["Message"])

        endpoint_url =  data_sns["endpoint_url"] 
        connectionId = data_sns["connectionId"]

        # Verifica se a ação recebida é "sendMessage-sns"
        action = data_sns.get("action", "")
        if action != "sendMessage-sns":
            return {"statusCode": 400, "body": json.dumps({"message": "Ação inválida."})}

        # Obtém o nome do personagem
        personagem = data_sns.get("character", "").strip()
        if not personagem:
            return {"statusCode": 400, "body": json.dumps({"message": "O campo 'character' é obrigatório."})}

        # Recupera a chave da OpenAI
        openai_api_key = get_secret()

        # Instancia o modelo LLM
        llm = get_llm_model(openai_api_key)

        # Busca informações do personagem no DynamoDB
        dados_personagem = buscar_personagem_dynamodb(personagem)

        if not dados_personagem:
            response_message = f"Nenhum dado encontrado para {personagem}. Tente outro nome."
        else:
            historia = gerar_historia(llm, personagem, dados_personagem)
            response_message = historia

        api_gateway_client = boto3.client('apigatewaymanagementapi', 
        endpoint_url = endpoint_url)

        api_gateway_client.post_to_connection(ConnectionId=connectionId, 
                                              Data=json.dumps(response_message, ensure_ascii=False).encode('utf-8'))


        return {
            "statusCode": 200,
            "body": json.dumps({"message": response_message}, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Erro durante a execução da Lambda: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro interno na função Lambda."})
        }
