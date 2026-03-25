import requests

class VtexService:
    def __init__(self, config):
        self.loja = config['loja']
        self.headers = {
            'X-VTEX-API-AppKey': config['appKey'],
            'X-VTEX-API-AppToken': config['appToken'],
            'Content-Type': 'application/json'
        }
        self.base_url = f"https://{self.loja}.vtexcommercestable.com.br/api/oms/pvt/orders"

    def fetch_orders_list(self, start_date, end_date, page=1):
        params = {
            'per_page': 100,
            'page': page,
            'f_creationDate': f"creationDate:[{start_date} TO {end_date}]",
            'orderBy': 'creationDate,asc'
        }
        response = requests.get(self.base_url, headers=self.headers, params=params)
        return response.json() if response.status_code == 200 else None

    def fetch_order_detail(self, order_id):
        url = f"{self.base_url}/{order_id}"
        response = requests.get(url, headers=self.headers, timeout=30)
        return response.json() if response.status_code == 200 else None