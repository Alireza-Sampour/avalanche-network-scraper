import asyncio
import aiohttp
import time
import requests
from bs4 import BeautifulSoup


def save_to_file(file_name: str, data: list) -> None:
    with open(file_name, 'w') as f:
        for item in data:
            print(f"From: {item['from']}, Count: {item['count']}, Volume: {item['volume'] / 1000:.3f} K", file=f)


# summarize data based on from address
def summarize_data(result: list, full_details: dict) -> None:
    grouped_result = {}
    for item in result:
        # filter out the data that is not in the selected pair token
        if (tmp := full_details.get(item['hash'])) is not None and tmp['tokens'] == selected_pair_token:
            if item['from'] not in grouped_result: # create a new entry
                grouped_result[item['from']] = {'from': item['from'], 'count': 0, 'volume': 0.0}
            grouped_result[item['from']]['count'] += 1 
            grouped_result[item['from']]['volume'] += float(full_details[item['hash']]['volume']) 

    sorted_result = sorted(grouped_result.values(), key=lambda x: x[sort_by], reverse=True) # sort data
    top_n = sorted_result[:top_nth] # get top_nth

    # save data to file
    save_to_file(fname, top_n)


async def get(url: str, session: aiohttp.ClientSession) -> bool:
    try:
        async with session.get(url=url, headers=avascan_headers, timeout=time_out) as response:
            resp = await response.read() # read the response
            await asyncio.sleep(0)
            if response.status == 200:
                tx_hash = url.split('/')[-1] # get the tx hash
                soup = BeautifulSoup(resp, 'lxml')
                from_address = soup.select_one('div.row:nth-child(3) > div:nth-child(1) > div:nth-child(1)') # get the from address
                if from_address is not None:
                    from_address = from_address.div.text.strip() 
                else:
                    from_address = 'N/A'
                if len(tmp := soup.find_all('div', class_='transfer')) >= 2:
                    txs_detail[tx_hash] = {'from': from_address} # add the from address
                    txs_detail[tx_hash]['tokens'] = set([tmp[0].find('span', class_='transfer-asset').text.strip().upper()]) # add the token
                    txs_detail[tx_hash]['tokens'].add(tmp[-1].find('span', class_='transfer-asset').text.strip().upper()) # add the token
                    txs_detail[tx_hash]['volume'] = (float((tmp[0].find('span', class_='amount').text.strip().replace(',', '')))) # add the volume
                elif len(tmp) == 1:
                    txs_detail[tx_hash] = {'from': from_address}
                    txs_detail[tx_hash]['tokens'] = set([tmp[0].find('span', class_='transfer-asset').text.strip().upper()])
                    txs_detail[tx_hash]['tokens'].add('AVAX'.upper())
                    txs_detail[tx_hash]['volume'] = (float((tmp[0].find('span', class_='amount').text.strip().replace(',', ''))))
                else:
                    print(f">>> Error: No data found for {tx_hash}")
                return True 
            else:
                print(f">>> Failed to get url {url} with the response code: {response.status}.")
    except Exception as e:
        print(f">>> Unable to get url {url} due to {e.__class__}.")
    return False


async def main(urls: list) -> None:
    async with aiohttp.ClientSession(timeout=time_out) as session:
        await asyncio.gather(*[get(url, session) for url in urls])
    print("Finalized all. Return is a list of length {} outputs.".format(len(txs_detail)))


contract_address = "Contract-Address" # Contract address
api_key = "API-KEY" # API key for snowtrace.io
tx_detail_url = "https://avascan.info/blockchain/c/tx/" # URL for tx detail
tx_list_api = f"https://api.snowtrace.io/api?module=account&action=txlist&address={contract_address}&sort=dec&apikey={api_key}" # URL for tx list
time_stamp = 1645550732
top_nth = 50 # set this to get top nth data
sort_by = 'volume' # It can be `volume` or 'count'
selected_pair_token = ['USDT', 'USDC'] # Pair of tokens to be selected
fname = 'report.txt' # file name that data will be saved in
selected_pair_token = set(selected_pair_token)
txs_detail = {}

# cutsom timeout
time_out = aiohttp.ClientTimeout(
    total = None,
    sock_connect = 10,
    sock_read = 10
)

avascan_headers = {
    'Host': 'avascan.info',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://avascan.info/',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Cache-Control': 'max-age=0',
    'TE': 'trailers'
}

response = requests.get(tx_list_api, timeout=30) # Get tx list from snowtrace.io
if response.status_code == 200:
    result = response.json()['result']
    urls = [tx_detail_url + item['hash'] for item in result if item['isError'] != '1' and int(item['timeStamp']) >= time_stamp] # Build urls for tx detail (ingore failed txs and filter timestamp)
    start = time.time()
    asyncio.run(main(urls))
    end = time.time()
    print(f"Took: {end - start} seconds to request {len(urls)} links.")
    summarize_data(result, txs_detail)
else:
    print(f"API Error: {response.status_code}")
