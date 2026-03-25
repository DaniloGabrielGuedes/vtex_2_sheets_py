import pandas as pd

class DataProcessor:
    def __init__(self):
        self.column_mapping = {
            "data": "Início do intervalo",
            "filtro": "Nome da fila",
            "oferta": "Oferta",
            "resposta": "Resposta",
            "abandono": "Abandono"
        }

    def process_dataframes(self, list_of_dfs):
        if not list_of_dfs:
            return None
        
        # Consolida todos os dataframes em um só
        df_full = pd.concat(list_of_dfs, ignore_index=True)

        # 1. Filtra apenas o que contém "industrial" (case insensitive)
        df_filtered = df_full[df_full[self.column_mapping['filtro']].str.contains("industrial", case=False, na=False)].copy()

        # 2. Trata a Data (converte para objeto datetime)
        df_filtered['Data_Clean'] = pd.to_datetime(df_filtered[self.column_mapping['data']]).dt.date

        # 3. Agrupa e Soma (Group By)
        resumo = df_filtered.groupby('Data_Clean').agg({
            self.column_mapping['oferta']: 'sum',
            self.column_mapping['resposta']: 'sum',
            self.column_mapping['abandono']: 'sum'
        }).reset_index()

        # 4. Cálculos de Porcentagem
        resumo['% de Resposta'] = resumo[self.column_mapping['resposta']] / resumo[self.column_mapping['oferta']]
        resumo['% de Abandono'] = resumo[self.column_mapping['abandono']] / resumo[self.column_mapping['oferta']]
        
        # Preenche NAs resultantes de divisão por zero
        resumo = resumo.fillna(0)

        # 5. Ordena por Data
        resumo = resumo.sort_values(by='Data_Clean')
        
        return resumo