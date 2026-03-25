import streamlit as st
import os
import time
import traceback
from datetime import timedelta
from google.oauth2 import service_account

from src.service.sheets_service import SheetsService
from src.service.vtex_service import VtexService
from src.processor.vtex_processor import VtexProcessor
from src.service.drive_service import DriveService

st.set_page_config(page_title="VTEX Integration", layout="wide")

google_credentials = service_account.Credentials.from_service_account_file(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)

with st.sidebar:
    st.header("Configurações do Processo")
    folder_id = st.text_input("ID da Pasta (Drive)", value=st.secrets.get("DEFAULT_FOLDER_ID", ""))
    
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
    tempo_inicio_total = time.time()
    
    try:
        sheets_service = SheetsService(google_credentials, selected_sheet_id)
        
        with st.spinner("Lendo configurações da planilha..."):
            vtex_config = sheets_service.get_vtex_config()
        
        if not vtex_config['loja']:
            st.error("Configurações VTEX não encontradas.")
            st.stop()

        vtex_service = VtexService(vtex_config)
        processor = VtexProcessor()
        
        cols = ["Data", "Pedido", "Status", "IDProduto", "Produto", "Quantidade", "ValorUnitario", "ValorTotal"]
        ws = sheets_service.prepare_sheet("Dados_Atualizados", cols)
        
        lista_dias = []
        data_atual = data_inicio
        while data_atual <= data_fim:
            lista_dias.append(data_atual)
            data_atual += timedelta(days=1)
        
        total_dias = len(lista_dias)
        st.info(f"Iniciando processamento de {total_dias} dias...")

        status_log = st.empty()
        prog_bar = st.progress(0.0)
        prog_text = st.empty()
        
        linhas_totais = 0

        # LOOP DIÁRIO
        for idx, dia in enumerate(lista_dias):
            dia_str = dia.strftime("%d/%m/%Y")
            dt_ini_vtex = dia.strftime("%Y-%m-%dT00:00:00Z")
            dt_fim_vtex = dia.strftime("%Y-%m-%dT23:59:59Z")
            
            status_log.markdown(f"📅 **Processando dia:** {dia_str} ({idx + 1}/{total_dias})")
            
            summaries_do_dia = []
            page = 1
            while True:
                res = vtex_service.fetch_orders_list(dt_ini_vtex, dt_fim_vtex, page)
                if not res or not res.get('list'): break
                summaries_do_dia.extend(res['list'])
                if page >= res['paging']['pages']: break
                page += 1
            
            if summaries_do_dia:
                def up_bar_dia(percent):
                    global_percent = (idx + percent) / total_dias
                    prog_bar.progress(min(global_percent, 1.0))
                    prog_text.text(f"Dia {dia_str}: {int(percent * 100)}% concluído")

                data_rows = processor.process_all(summaries_do_dia, vtex_service, up_bar_dia)
                
                if data_rows:
                    sheets_service.append_data(ws, data_rows)
                    linhas_totais += len(data_rows)
            else:
                prog_bar.progress((idx + 1) / total_dias)

        tempo_final_total = time.time()
        duracao = tempo_final_total - tempo_inicio_total

        m, s = divmod(int(duracao), 60)
        tempo_formatado = f"{m}min {s}s" if m > 0 else f"{s}s"

        st.success(f"✅ Processamento Concluído!")

        col_res1, col_res2 = st.columns(2)
        col_res1.metric("Linhas Processadas", linhas_totais)
        col_res2.metric("Tempo Total", tempo_formatado)
        
        st.balloons()

    except Exception as e:
        st.error(f"Erro Crítico: {repr(e)}")
        st.text(traceback.format_exc())