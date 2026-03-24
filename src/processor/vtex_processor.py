import aiohttp
import asyncio
from datetime import datetime

class VtexProcessorAsync:
    @staticmethod
    def format_date(vtex_date):
        dt = datetime.fromisoformat(vtex_date.replace('Z', '+00:00'))
        return dt.strftime("%d/%m/%Y %H:%M:%S")

    async def process_order(self, order, session, vtex_service, semaphore):
        status = order.get('statusDescription', '')

        if status in ["Cancelado", "Cancelamento Solicitado"]:
            return []

        async with semaphore:
            detail = await vtex_service.fetch_order_detail(session, order['orderId'])

        rows = []
        if detail and 'items' in detail:
            creation_date = order['creationDate']

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

    async def process_all(self, orders_list, vtex_service, progress_callback=None):
        formated_orders = []
        total = len(orders_list)

        semaphore = asyncio.Semaphore(5)

        async with aiohttp.ClientSession() as session:
            tasks = [
                self.process_order(order, session, vtex_service, semaphore)
                for order in orders_list
            ]

            for i, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                formated_orders.extend(result)

                if progress_callback:
                    progress_callback(i + 1, total)
        
        formated_orders.sort(key=lambda x: x[0])
        
        return formated_orders