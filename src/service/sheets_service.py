import gspread

class SheetsService:
    def __init__(self, credentials, spreadsheet_id):
        self.client = gspread.authorize(credentials)
        self.ss = self.client.open_by_key(spreadsheet_id)

    def get_vtex_config(self):
        
        sheet = self.ss.get_worksheet(0) 
        return {
            "loja": sheet.acell('B2').value,
            "appKey": sheet.acell('B3').value,
            "appToken": sheet.acell('B4').value
        }

    def prepare_sheet(self, sheet_name, headers):
        try:
            worksheet = self.ss.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.ss.add_worksheet(title=sheet_name, rows="100", cols="20")
        
        worksheet.clear()
        worksheet.update('A1', [headers])
        
        worksheet.format("A1:H1", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8}})
        return worksheet

    def append_data(self, worksheet, data):
        if data:
            string_data = [[str(cell) for cell in row] for row in data]
            worksheet.append_rows(string_data)