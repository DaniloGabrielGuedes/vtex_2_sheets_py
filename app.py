import streamlit as st
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
import pandas as pd
import traceback

from src.service.sheets_service import SheetsService
from src.service.vtex_service import VtexService
from src.processor.vtex_processor import VtexProcessor
from src.service.drive_service import DriveService

load_dotenv()

st.set_page_config(page_title="VTEX Integration", layout="wide")

google_credentials = service_account.Credentials.from_service_account_file(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)

with st.sidebar:
    st.header("Configurações do Processo")
    folder_id = st.text_input("ID da Pasta (Drive)", value=os.getenv("DEFAULT_FOLDER_ID", ""))
    
    st.divider()
    st.subheader("Período de Busca")
    data_inicio = st.date_input("Data Início")
    data_fim = st.date_input("Data Fim")

st.title("📑 Integração VTEX -> Google Sheets")

drive_mgr = DriveService(google_credentials)
sheets_list = drive_mgr.service.files().list(
    q="mimeType = 'application/vnd.google-apps.spreadsheet'", 
    fields="files(id, name)"
).execute().get('files', [])

sheet_options = {f"{s['name']}": s['id'] for s in sheets_list}
if not sheet_options:
    st.warning("Nenhuma planilha encontrada ou usuário sem acesso a pasta. Por favor, recarregue a página para tentar novamente.")
    st.stop()

selected_sheet_name = st.selectbox("Selecione a Planilha que contém as configurações:", options=list(sheet_options.keys()))
selected_sheet_id = sheet_options[selected_sheet_name]

if st.button("🚀 Iniciar Processamento", type="primary"):
    try:
        sheets_service = SheetsService(google_credentials, selected_sheet_id)
        
        with st.spinner("Lendo configurações da planilha..."):
            vtex_config = sheets_service.get_vtex_config()
            if vtex_config['loja']: st.info(f"Conectado à loja: {vtex_config['loja']}")
        
        if not vtex_config['loja']:
            st.error("Configurações VTEX não encontradas ou incompletas na planilha. Verifique se as chaves e suas formatações estão corretas.")
            st.stop()

        vtex_service = VtexService(vtex_config)

        with st.spinner("Buscando pedidos na VTEX..."):
            dt_ini = data_inicio.strftime("%Y-%m-%dT00:00:00Z")
            dt_fim = data_fim.strftime("%Y-%m-%dT23:59:59Z")

            all_summaries = []
            page = 1
            while True:
                res = vtex_service.fetch_orders_list(dt_ini, dt_fim, page)
                if not res or not res.get('list'): break
                all_summaries.extend(res['list'])
                if page >= res['paging']['pages']: break
                page += 1

        if not all_summaries:
            st.warning("Nenhum pedido encontrado no período.")
            st.stop()

        prog_bar = st.progress(0.0)
        prog_text = st.empty()
        
        def up_bar(percent):
            prog_bar.progress(percent)
            prog_text.text(f"Processando: {int(percent * 100)}%")
        
        processor = VtexProcessor()
        data_rows = processor.process_all(all_summaries, vtex_service, up_bar)
        
        # Converte para DataFrame e envia ao Sheets
        cols = ["Data", "Pedido", "Status", "IDProduto", "Produto", "Quantidade", "ValorUnitario", "ValorTotal"]
        df_final = pd.DataFrame(data_rows, columns=cols)
        
        sheets_service.write_report(df_final)
        st.success(f"Sucesso! {len(df_final)} linhas processadas e enviadas para 'Dados_Atualizados'.")
        st.balloons()

    except Exception as e:
        st.error(f"Erro: {repr(e)}")
        st.text(traceback.format_exc())