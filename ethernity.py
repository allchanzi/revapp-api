#!/usr/bin/python3

import time
import argparse
import ipfshttpclient
import ntpath
import os
import sys
import socket
from datetime import datetime

from web3 import Web3
from eth_account import Account
from web3.middleware import geth_poa_middleware
from web3.exceptions import (
    BlockNotFound,
    TimeExhausted,
    TransactionNotFound,
)

class etnyPoX:
    def __init__(self, acc, key, script, fileset, image='QmYF7WuHAH4tr896YXxwahaBEWT6YPcagB1dpotGWtCbwS:etny-pynithy', client=None):
        status = False
        self.address     = format(acc)
        self.privatekey  = format(key)
        self.cpu         = 1
        self.memory      = 1
        self.storage     = 1
        self.bandwidth   = 1
        self.duration    = 10
        self.instances   = 1
        self.imageHash   = image
        self.scriptHash  = script
        self.filesetHash = fileset
        self.ipfs_client = client

        f = open(os.path.dirname(os.path.realpath(__file__)) + '/pox.abi')
        self.contract_abi = f.read()
        f.close()

        self.w3 = Web3(Web3.HTTPProvider("https://core.bloxberg.org"))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        self.acct = Account.privateKeyToAccount(self.privatekey)
        self.etny = self.w3.eth.contract(address=self.w3.toChecksumAddress("0x99738e909a62e2e4840a59214638828E082A9A2b"), abi=self.contract_abi)

        self.dorequest = 0
        self.dohash = 0

        status = True

    def add_request(self):
        nonce = self.w3.eth.getTransactionCount(self.address)

        print(datetime.now(), "Sending payload to IPFS...")

        unicorn_txn = self.etny.functions._addDORequest(
            self.cpu, self.memory, self.storage, self.bandwidth, self.duration, self.instances, 0, self.imageHash, self.scriptHash, self.filesetHash, ""
        ).buildTransaction({
            'gas': 1000000,
            'chainId': 8995,
            'nonce': nonce,
            'gasPrice': self.w3.toWei("1", "mwei"),
        })

        signed_txn = self.w3.eth.account.sign_transaction(unicorn_txn, private_key=self.acct.key)
        self.w3.eth.sendRawTransaction(signed_txn.rawTransaction)
        hash = self.w3.toHex(self.w3.sha3(signed_txn.rawTransaction))

        for i in range(100):
            try:
                receipt = self.w3.eth.waitForTransactionReceipt(hash)
                processed_logs = self.etny.events._addDORequestEV().processReceipt(receipt)
                self.dorequest = processed_logs[0].args._rowNumber
            except KeyError:
                time.sleep(1)
                continue
            except:
                raise
            else:
                print(datetime.now(), "Request %s created successfuly!" % self.dorequest)
                print(datetime.now(), "TX Hash: %s" % hash)
                self.dohash = hash
                break

        if (receipt == None):
            print(datetime.now(), "Unable to create request, please check conectivity with bloxberg node")
            sys.exit()

    def wait_for_processor(self):
        while True:
            order = self.find_order(self.dorequest)
            if order is not None:
                return self.approve_order(order)
            else:
                time.sleep(5)

    def approve_order(self, order):
        nonce = self.w3.eth.getTransactionCount(self.address)

        unicorn_txn = self.etny.functions._approveOrder(order).buildTransaction({
            'gas': 1000000,
            'chainId': 8995,
            'nonce': nonce,
            'gasPrice': self.w3.toWei("1", "mwei"),
        })

        signed_txn = self.w3.eth.account.sign_transaction(unicorn_txn, private_key=self.acct.key)
        self.w3.eth.sendRawTransaction(signed_txn.rawTransaction)
        hash = self.w3.toHex(self.w3.sha3(signed_txn.rawTransaction))

        try:
            self.w3.eth.waitForTransactionReceipt(hash)
        except:
            raise

        return self.get_results_from_order(order)

    def get_results_from_order(self, order):
        print(datetime.now(), "Waiting for task to finish...")
        while True:
            result = 0
            try:
                result_hash = self.etny.caller(transaction={'from': self.address})._getResultFromOrder(order)
            except:
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(5)
                continue
            else:

                if self.ipfs_client is None:
                    self.ipfs_client = ipfshttpclient.connect('/dns/ipfs.infura.io/tcp/5001/https')

                while True:
                    try:
                        result = self.ipfs_client.cat(result_hash)
                    except:
                        sys.stdout.write('.')
                        sys.stdout.flush()
                        time.sleep(1)
                        continue
                    else:
                        break

                transaction = self.w3.eth.getTransaction(self.dohash)
                block = self.w3.eth.getBlock(transaction['blockNumber'])
                blocktimestamp = (block['timestamp'])
                blockdatetime = datetime.fromtimestamp(blocktimestamp)
                endBlockNumber = self.w3.eth.blockNumber
                startBlockNumber = endBlockNumber - 20

                for i in range(endBlockNumber, startBlockNumber, -1):
                    block = self.w3.eth.getBlock(i, True)
                    if block is not None and block.transactions is not None:
                        transactions = block["transactions"]
                        for transaction in transactions:
                            if transaction["to"] == "0x99738e909a62e2e4840a59214638828E082A9A2b":
                                input = self.etny.decode_function_input(transaction.input)
                                function = input[0]
                                params = input[1]
                                if "_addResultToOrder" in function.fn_name and params['_orderItem'] == order:
                                    resulthash = params['_result']
                                    resulttransactionhash = transaction['hash']
                                    resultblock = self.w3.eth.getBlock(transaction['blockNumber'])
                                    resultblocktimestamp = (block['timestamp'])
                                    resultblockdatetime = datetime.fromtimestamp(resultblocktimestamp)

                return {'txIn': self.dohash, 'txOut': str(resulttransactionhash.hex()), 'image': self.imageHash, 'script': self.scriptHash, 'fileset': self.filesetHash, 'result': result.decode('utf-8')}

    def find_order(self, doReq):
        sys.stdout.write('.')
        sys.stdout.flush()
        count = self.etny.functions._getOrdersCount().call()
        for i in range(count - 1, count - 5, -1):
            order = self.etny.caller()._getOrder(i)
            if order[2] == self.dorequest and order[4] == 0:
                return i
        return None
