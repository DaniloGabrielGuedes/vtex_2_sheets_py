import gspread

class SheetsService:
    def __init__(self, credentials, spreadsheet_id):
        self.client = gspread.authorize(credentials)
        self.ss = self.client.open_by_key(spreadsheet_id)

    def get_vtex_config(self):
        """Lê as configurações da primeira aba (ou aba ativa)"""
        
        sheet = self.ss.get_worksheet(0) 
        return {
            "loja": sheet.acell('B2').value,
            "appKey": sheet.acell('B3').value,
            "appToken": sheet.acell('B4').value
        }

    def write_report(self, df, sheet_name="Dados_Atualizados"):
        """Limpa e escreve os dados na aba de destino"""
        try:
            worksheet = self.ss.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.ss.add_worksheet(title=sheet_name, rows="100", cols="20")

        worksheet.clear()
        # Prepara cabeçalho e dados
        data = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        worksheet.update('A1', data)
        
        # Formatação básica (Opcional, similar ao seu JS)
        worksheet.format("A1:H1", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}})