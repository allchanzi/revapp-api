from ethernity import etnyPoX

import ipfshttpclient
import json
from web3 import Web3
from web3.logs import (IGNORE,)
import os
from queue import Queue
import requests
import time
import shutil
from threading import Thread

#-------------TO CHANGE---------------------------------
INFURA_KEY    = os.environ['INFURA_KEY']
ETHERNITY_ACC = os.environ['ETHERNITY_ACC']
ETHERNITY_KEY = os.environ['ETHERNITY_KEY']
ROPSTEN_ACC   = os.environ['ROPSTEN_ACC']
ROPSTEN_KEY   = os.environ['ROPSTEN_KEY']
API_KEY       = os.environ['API_KEY']

ETH_GATEWAY    = 'https://ropsten.infura.io/v3/' + INFURA_KEY
IPFS_GATEWAY   = '/dns/ipfs.infura.io/tcp/5001/https'
ACCOUNT        = ROPSTEN_ACC
PRIVATE_KEY    = ROPSTEN_KEY
SMART_CONTRACT = '0x156A18fAfb14197Cdf216396B536147AAF0ec32d'
ALGORITHM_HASH = None
ipfshttpclient.VERSION_MAXIMUM = '0.9.0'

GLOBAL_QUEUE = Queue()

class Handler(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.inq = GLOBAL_QUEUE  # declare a queue for the server

    def run(self):
        while True:
            result = self.inq.get()   # get a message from input queue
            if not result:
                time.sleep(1)
                continue
            review_id, algorithm_hash, product_reviews, author_reviews, ean = result[0]['args'].values()

            review_data = {"reviews": {rev_id: get_review_data(review_id=rev_id) for rev_id in set((product_reviews + author_reviews + [review_id]))}}
            data_hash = save_data_to_ipfs(review_data)

            if check_ipfs_hash(ALGORITHM_HASH):
                th = Thread(target=get_result, args=(ALGORITHM_HASH, data_hash, ean)).start()

def handle_event(event):
    e = None
    receipt = w3.eth.waitForTransactionReceipt(event['transactionHash'])
    result = contract.events['InsertedReview']().processReceipt(receipt, IGNORE)
    GLOBAL_QUEUE.put(result)

def log_loop(event_filter, poll_interval):
    while True:
        for event in event_filter.get_new_entries():
            handle_event(event)
            time.sleep(poll_interval)

def check_ipfs_hash(hash):
    print("Evaluate: ", hash)
    try:
        client.cat(hash)
        return True
    except Exception as e:
        print(e)
        return False

def get_file_from_ipfs(hash):
    return json.loads(client.cat(hash).decode("utf-8"))

def get_review_data(review_id: int) -> dict:
    return get_file_from_ipfs(contract.functions.getReviewById(review_id).call()[0])

def save_data_to_ipfs(data: dict) -> str:
    shutil.rmtree('fileset', ignore_errors=True)
    os.mkdir('fileset')
    with open('fileset/reviews.json', "x") as file:
        json.dump(data, file)
    response = client.add('fileset', recursive=True)
    hash = None
    for res in response:
        print(res)
        if res.get('Name', None) == "fileset":
            hash = res.get('Hash')
    return hash

def write_result_to_ipfs(data) -> str:
    return client.add_json(data)

def write_hash_to_blockchain(results):
    nonce = w3.eth.getTransactionCount(ACCOUNT, 'pending')
    for i, result in enumerate(json.loads(results.get('result'))):
        review_id = result.get('id')
        result_json = {k: v for k, v in result.items() if k not in ('id', )}
        result_json['txIn'] = results.get("txIn", "")
        result_json['txOut'] = results.get("txOut", "")
        result_hash = write_result_to_ipfs(result_json)
        send_update(int(review_id), result_hash, nonce + i)


def get_price():
    response = requests.get("https://ethgasstation.info/api/ethgasAPI.json?api-key=" + API_KEY)
    return json.loads(response.content.decode('utf8')).get("fastest")

def get_result(algorithm_hash, data_hash, ean):
    request = etnyPoX(script=algorithm_hash, fileset=data_hash, client=client, acc=ETHERNITY_ACC, key=ETHERNITY_KEY)
    request.add_request()
    result = request.wait_for_processor()
    write_hash_to_blockchain(result)

def connect_IPFS_gateway():
    while True:
        try:
            client = ipfshttpclient.connect(IPFS_GATEWAY)
            return client
        except Exception as e:
            print(e)
            time.sleep(1)

def load_smart_contract():
    while True:
        print('Connecting to Smart Contract...')
        try:
            with open(os.path.dirname(os.path.realpath(__file__)) + '/RevApp.abi') as file:
                contract_abi = file.read()
            contract_address = SMART_CONTRACT
            contract = w3.eth.contract(address=contract_address, abi=contract_abi)
            return contract
        except Exception as e:
            print(e)
            time.sleep(1)

def send_update(id, hash, nonce):
    for i in range(20):
        try:
            unicorn_txn = contract.functions.updateReviewResult(int(id), hash).buildTransaction({
                'gas': 1000000,
                'gasPrice': 50000000000,
                'chainId': 3,
                'nonce': nonce,
            })

            signed_txn = w3.eth.account.sign_transaction(unicorn_txn, private_key=PRIVATE_KEY)
            w3.eth.sendRawTransaction(signed_txn.rawTransaction)
            tx_hash = w3.sha3(signed_txn.rawTransaction)
            receipt = w3.eth.waitForTransactionReceipt(tx_hash)

        except Exception as e:
            print()
            return



if __name__ == '__main__':
    print('Connecting to IPFS Gateway...')
    client = connect_IPFS_gateway()
    ALGORITHM_HASH = client.add('detect_main.py')['Hash']
    print('Loading RevApp contract ...')
    w3 = Web3(Web3.HTTPProvider(ETH_GATEWAY))
    contract = load_smart_contract()
    print('RevApp contract loaded OK => ', SMART_CONTRACT)
    print("Creating Handler thread")
    hth = Handler()
    hth.start()
    print("Handler thread running")
    print("Start listening ...")
    block_filter = w3.eth.filter({'fromBlock': 'latest', 'address': SMART_CONTRACT})
    log_loop(block_filter, 30)
    print("Listener stopped")
    print("Waiting for Handler thread")
    hth.join()
    print("Handler thread joined. Exiting.")
