from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from supabase import create_client, Client
import fitz  # PyMuPDF
import pandas as pd
import re
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
async def upload_pdf(file: UploadFile = File(None), url: str = Query(None)):
    try:
        if url:
            # Ler o PDF a partir do link
            response = requests.get(url)
            response.raise_for_status()
            conteudo_pdf = response.content
        elif file:
            # Ler o conteúdo do arquivo PDF
            conteudo_pdf = await file.read()
        else:
            raise HTTPException(status_code=400, detail="Nenhum arquivo ou URL fornecido.")

        # Extrair texto do PDF
        text = extract_text_from_pdf(conteudo_pdf)
        last_page_text = extract_last_page_text_from_pdf(conteudo_pdf)
        cnpj, company_name, report_type, period_start, period_end = extract_header_details(text, last_page_text)
        df = process_text_to_dataframe(text, cnpj, company_name, report_type, period_start, period_end)
        
        # Gravar os dados extraídos em uma tabela do Supabase
        nome_tabela = 'balancete'
        data_dict = df.to_dict(orient='records')
        
        #verificar se existe e deletar para criar notavmente        
        # Deletar registros onde a condição é atendida
        condition_field = "period_start"
        condition_value = period_start
        condition_field2 = "cnpj"
        condition_value2 = cnpj
        resposta_supabase = supabase.from_(nome_tabela).delete().eq(condition_field, condition_value).eq(condition_field2, condition_value2).execute()
        
        # Inserção dos dados no Supabase
        resposta_supabase = supabase.from_(nome_tabela).insert(data_dict).execute()
        
        # Definir o cabeçalho de autorização
        cabecalho_autorizacao = {
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/octet-stream"
        }

        # URL do endpoint do Supabase para upload de arquivo
        if file:
            url_upload = f"{supabase_url}/storage/v1/object/{bucket_name}/{file.filename}"
        else:
            url_upload = f"{supabase_url}/storage/v1/object/{bucket_name}/file_from_link.pdf"

        # Enviar a requisição para fazer o upload do arquivo PDF
        resposta = requests.post(url_upload, headers=cabecalho_autorizacao, data=conteudo_pdf)

        return {
            "mensagem": "Os dados do PDF foram gravados com sucesso no Revizzor.",
            "supabase_data": resposta_supabase,
            "storage_status_code":  resposta.status_code
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

def extract_last_page_text_from_pdf(pdf_content):
    doc = fitz.open(stream=pdf_content, filetype="pdf")
    last_page = doc[-1]  # Obter a última página
    text = last_page.get_text()
    doc.close()
    return text

def extract_header_details(text, last_page_text):
    # Regex para capturar CNPJ, nome da empresa com código, período
    cnpj_match = re.search(r"CNPJ:\s*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", text)
    #company_name_match = re.search(r"(\d{4})\s+([\w\s&-]+(?:LTDA|ME|S/A|SA|EPP|EIRELI))", text, re.IGNORECASE)
    company_name_match = re.search(r"(\d{4})\s+(.+?)\s+CNPJ:", text, re.DOTALL)
    period_match = re.search(r"Período:\s*(\d{2}/\d{2}/\d{4})\s*a\s*(\d{2}/\d{2}/\d{4})", text)
    
    cnpj = cnpj_match.group(1) if cnpj_match else ""
    company_code = company_name_match.group(1) if company_name_match else ""
    company_name = company_name_match.group(2).strip() if company_name_match else ""
    full_company_name = f"{company_code} {company_name}" if company_code else company_name
    period_start = period_match.group(1) if period_match else ""
    period_end = period_match.group(2) if period_match else ""
    
    return cnpj, full_company_name, "Balancete", period_start, period_end

def process_text_to_dataframe(text, cnpj, company_name, report_type, period_start, period_end):
    # Pre-process the text to handle multiline records
    lines = text.split('\n')
    combined_lines = []
    current_line = ""
    
    for line in lines:
        if re.match(r'^\d{1,8}\s', line):
            if current_line:
                combined_lines.append(current_line)
            current_line = line
        else:
            current_line += " " + line.strip()
    
    if current_line:
        combined_lines.append(current_line)
    
    # Regex pattern to match lines correctly, including lines with parentheses
    pattern = re.compile(r"(\d{1,8})\s+(S)?\s*([\d\.]+(?:\.\d+)?(?:\s+\d+\.\d+)?\s*[^ \d][^\s\d].*?)\s+([\d\.,\-\(\)]+)\s+([\d\.,\-\(\)]+)\s+([\d\.,\-\(\)]+)\s+([\d\.,\-\(\)]+)")
    data = []
    
    for line in combined_lines:
        matches = pattern.findall(line)
        for match in matches:
            conta = match[0]
            s = match[1]
            classificacao = match[2].strip()
            saldo_ant = float(match[3].replace(".", "").replace(",", ".").replace("(", "-").replace(")", ""))
            debito = float(match[4].replace(".", "").replace(",", ".").replace("(", "-").replace(")", ""))
            credito = float(match[5].replace(".", "").replace(",", ".").replace("(", "-").replace(")", ""))
            saldo = float(match[6].replace(".", "").replace(",", ".").replace("(", "-").replace(")", ""))
            
            data.append([cnpj, company_name, report_type, period_start, period_end, conta, s, classificacao, saldo_ant, debito, credito, saldo])
    
    columns = ["cnpj", "company_name", "report_type", "period_start", "period_end", "conta", "ContaSumario", "classificacao", "saldo_ant", "debito", "credito", "saldo"]
    df = pd.DataFrame(data, columns=columns)
    
    return df