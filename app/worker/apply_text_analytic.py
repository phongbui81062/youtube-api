import aiohttp
import asyncio


async def get_data(i: str, text: str, session: aiohttp.ClientSession):
    headers = {
        'Ocp-Apim-Subscription-Key': '102d0401e9a440ecba6e739075a5def6',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    body = {
        "documents":
        [
            {
                "language": "en",
                "id": i,
                "text": text
            }
        ]
    }
    url = "https://phongbui.cognitiveservices.azure.com/text/analytics/v3.1-preview.1/sentiment?showStats=true&opinionMining=true"
    async with session.post(url, headers=headers, data=str(body)) as respones:
        content = await respones.json()
        if respones.status == 200:
            return content
        else:
            return None
