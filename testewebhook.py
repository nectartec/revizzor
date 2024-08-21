import requests

# URL do webhook do Make
webhook_url = "https://hook.us1.make.com/yurro391iifjevu4uftiuqyombaellva"

# Dados que você quer enviar para o webhook
data = {
    "cnpj": "61.971.040/0001-75" 
}

# Fazendo a solicitação POST
response = requests.post(webhook_url, json=data)

# Verificando a resposta e extraindo o link
if response.status_code == 200:
    try:
        # Assumindo que o link está no campo 'link' do JSON retornado
        response_data = response.text
        
        print("Link completo retornado:")
        print(response_data)
    except ValueError:
        print(f"A resposta não está no formato. {response.status_code}")        
else:
    print(f"Erro ao chamar o webhook: {response.status_code} - {response.text}")
