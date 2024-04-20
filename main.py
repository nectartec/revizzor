from fastapi import FastAPI, File, UploadFile, HTTPException
from supabase_py import create_client
import fitz  # PyMuPDF
import pandas as pd
import re
from supabase_py.client import Client
import requests
import logging

app = FastAPI()

# Configurações do Supabase
supabase_url = "https://mnlopigqtaqafgefsbzc.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1ubG9waWdxdGFxYWZnZWZzYnpjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTE4Mzk2OTYsImV4cCI6MjAyNzQxNTY5Nn0.gRLXeRUyXbP5kZpAU4IbxBELKvjDPX14VtUfmYXyelY"
bucket_name = "revizzor/pdf"

# Criar cliente Supabase
supabase: Client = create_client(supabase_url, supabase_key)

# Configurar o logger
logging.basicConfig(level=logging.DEBUG)

# Rota para lidar com o upload do arquivo PDF
@app.post("/upload/pdf/")
async def upload_pdf(file: UploadFile = File(...)):
    try:
        # Ler o conteúdo do arquivo PDF
        conteudo_pdf = await file.read()

        # Extrair texto do PDF
        text = extract_text_from_pdf(conteudo_pdf)
        cnpj, company_name, report_type, period_start, period_end = extract_header_details(text)
        df = process_text_to_dataframe(text, cnpj, company_name, report_type, period_start, period_end)
        
        # Gravar os dados extraídos em uma tabela do Supabase
        nome_tabela = 'balancete'
        data_dict = df.to_dict(orient='records')

        # Inserção dos dados no Supabase
        resposta_supabase = supabase.from_(nome_tabela).insert(data_dict).execute()          

        # Definir o cabeçalho de autorização
        cabecalho_autorizacao = {
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/octet-stream"
        }

        # URL do endpoint do Supabase para upload de arquivo
        url_upload = f"{supabase_url}/storage/v1/object/{bucket_name}/{file.filename}"
        # Enviar a requisição para fazer o upload do arquivo PDF
        resposta = requests.post(url_upload, headers=cabecalho_autorizacao, data=conteudo_pdf)   

        return {
            "mensagem": "Os dados do PDF foram gravados com sucesso no Supabase."           
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Função para extrair texto de um arquivo PDF
def extract_text_from_pdf(pdf_content):
    doc = fitz.open(stream=pdf_content, filetype="pdf")
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    return text

def extract_header_details(text):
    # Regex para capturar CNPJ, nome da empresa e período
    cnpj_match = re.search(r"CNPJ:\s*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", text)
    # A busca agora inicia após os dígitos e uma quebra de linha até encontrar "CNPJ:"
    company_name_match = re.search(r"\n(\d{4}\s.*?)(?=\n\d|\nCNPJ:)", text, re.DOTALL)
    period_match = re.search(r"Período:\s*(\d{2}/\d{2}/\d{4})\s*a\s*(\d{2}/\d{2}/\d{4})", text)
    
    cnpj = cnpj_match.group(1) if cnpj_match else ""
    company_name = company_name_match.group(1).strip() if company_name_match else ""
    period_start = period_match.group(1) if period_match else ""
    period_end = period_match.group(2) if period_match else ""
    
    return cnpj, company_name, "Balancete", period_start, period_end

def process_text_to_dataframe(text, cnpj, company_name, report_type, period_start, period_end):
    pattern = r"(\d{1,8})\s+(S)?\s*([\d\.]+(?:\.\d+)?\s*[^ \d][^\s\d].*?)\s+([\d\.,]+\s*|\(\s*[\d\.,]+\s*\))\s+([\d\.,]+\s*|\(\s*[\d\.,]+\s*\))\s+([\d\.,]+\s*|\(\s*[\d\.,]+\s*\))\s+([\d\.,]+\s*|\(\s*[\d\.,]+\s*\))"
    matches = re.findall(pattern, text.replace("\n", " "))
    data = []
    for match in matches:
        conta, s, classificacao, saldo_ant, debito, credito, saldo = match
        classificacao = " ".join(classificacao.split())  # Normaliza espaços
        saldo_ant = float(saldo_ant.replace("(", "-").replace(")", "").replace(".", "").replace(",", "."))
        debito = float(debito.replace("(", "-").replace(")", "").replace(".", "").replace(",", "."))
        credito = float(credito.replace("(", "-").replace(")", "").replace(".", "").replace(",", "."))
        saldo = float(saldo.replace("(", "-").replace(")", "").replace(".", "").replace(",", "."))
        data.append([cnpj, company_name, report_type, period_start, period_end, conta, s, classificacao, saldo_ant, debito, credito, saldo])
    
    columns = ["cnpj", "company_name", "report_type", "period_start", "period_end", "conta", "ContaSumario", "classificacao", "saldo_ant", "debito", "credito", "saldo"]
    df = pd.DataFrame(data, columns=columns)
    return df
