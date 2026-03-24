import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

class DriveService:
    def __init__(self, credentials):
        self.service = build('drive', 'v3', credentials=credentials)

    def get_files_from_folder(self, folder_id):
        query = f"'{folder_id}' in parents and (mimeType = 'text/csv' or mimeType = 'application/vnd.google-apps.spreadsheet')"
        results = self.service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        return results.get('files', [])

    def download_csv_as_df(self, file_id):
        request = self.service.files().get_media(file_id=file_id)
        file_handle = io.BytesIO()
        downloader = MediaIoBaseDownload(file_handle, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_handle.seek(0)
        # Tenta ler com separador vírgula ou ponto-e-vírgula
        try:
            return pd.read_csv(file_handle, sep=',')
        except:
            file_handle.seek(0)
            return pd.read_csv(file_handle, sep=';')