from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class VtexProcessor:
    @staticmethod
    def format_date(vtex_date):
        dt = datetime.fromisoformat(vtex_date.replace('Z', '+00:00'))
        return dt.strftime("%d/%m/%Y %H:%M:%S")

    def process_order(self, order, vtex_service):
        status = order.get('statusDescription', '')
        if status in ["Cancelado", "Cancelamento Solicitado"]:
            return []

        detail = vtex_service.fetch_order_detail(order['orderId'])
        rows = []
        if detail and 'items' in detail:
            creation_date = self.format_date(order['creationDate'])
            for item in detail['items']:
                rows.append([
                    creation_date,
                    order['orderId'],
                    status,
                    item.get('id'),
                    item.get('name'),
                    item.get('quantity'),
                    item.get('price') / 100,
                    (item.get('price') * item.get('quantity')) / 100
                ])
        return rows

    def process_all(self, orders_list, vtex_service, progress_callback=None):
        formated_orders = []
        total = len(orders_list)

        with ThreadPoolExecutor(max_workers=10) as executor:

            futures = {executor.submit(self.process_order, order, vtex_service): order for order in orders_list}
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                formated_orders.extend(result)

                if progress_callback:
                    progress_callback((i + 1) / total)
        
        formated_orders.sort(key=lambda x: x[0])
        return formated_orders