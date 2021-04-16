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
            return {'statistics': {'documentsCount': 1,
                                   'validDocumentsCount': 1,
                                   'erroneousDocumentsCount': 0,
                                   'transactionsCount': 1},
                    'documents': [{'id': '0',
                                   'sentiment': 'neutral',
                                   'statistics': {'charactersCount': 28, 'transactionsCount': 1},
                                   'confidenceScores': {'positive': 0.36, 'neutral': 0.31, 'negative': 0.33},
                                   'sentences': [{'sentiment': 'neutral',
                                                  'confidenceScores': {'positive': 0.36,
                                                                       'neutral': 0.31,
                                                                       'negative': 0.33},
                                                  'offset': 0,
                                                  'length': 28,
                                                  'text': 'https://youtu.be/VzSKE2xVh7A',
                                                  'aspects': [],
                                                  'opinions': []}],
                                   'warnings': []}],
                    'errors': [],
                    'modelVersion': '2020-04-01'}
