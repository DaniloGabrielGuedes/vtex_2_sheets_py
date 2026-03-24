import asyncio
import aiohttp

class VtexServiceAsync:
    def __init__(self, config):
        self.base_url = f"https://{config['loja']}.vtexcommercestable.com.br/api/oms/pvt/orders"
        self.headers = {
            'X-VTEX-API-AppKey': config['appKey'],
            'X-VTEX-API-AppToken': config['appToken'],
            'Content-Type': 'application/json'
        }

    async def fetch_order_detail(self, session, order_id):
        url = f"{self.base_url}/{order_id}"

        for _ in range(3):
            try:
                async with session.get(url, headers=self.headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    
                    if resp.status == 429:
                        await asyncio.sleep(0.5)
                    else:
                        return None
            except Exception:
                await asyncio.sleep(0.5)

        return None
    
    async def fetch_all_orders(self, dt_ini, dt_fim):
        all_orders = []
        page = 1

        async with aiohttp.ClientSession() as session:
            while True:
                res = await self.fetch_orders_list(session, dt_ini, dt_fim, page)

                if not res or not res.get("list"):
                    break

                all_orders.extend(res["list"])

                if page >= res["paging"]["pages"]:
                    break

                page += 1

        return all_orders
    
    async def fetch_orders_list(self, session, dt_ini, dt_fim, page):
        url = f"{self.base_url}"

        params = {
            'per_page': 100,
            'page': page,
            'f_creationDate': f"creationDate:[{dt_ini} TO {dt_fim}]",
            'orderBy': 'creationDate,asc'
        }

        for _ in range(3):
            try:
                async with session.get(url, headers=self.headers, params=params) as resp:
                    if resp.status != 429:
                        text = await resp.text()
                        raise Exception(f"Erro VTEX {resp.status}: {text}")
                    
                    await asyncio.sleep(0.5)

            except Exception:
                await asyncio.sleep(0.5)

        return None