#!/usr/bin/env python3

import sys
from collections import defaultdict
from decimal import Decimal
import requests

CHAIN_ID = 1
SYMBOLS_TO_ADDRS = {
    "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    "stETH": "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
    "wstETH": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
    "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "XOR": "0x40fd72257597aa14c7231a7b1aaa29fce868f677",
    "RLB": "0x046eee2cc3188071c02bfc1745a6b17c656e3f3d",
    "VAL": "0xe88f8313e61a97cec1871ee37fbbe2a8bf3ed1e4",
    "PSWAP": "0x519c1001d550c0a1dae7d1fc220f7d14c2a521bb",
    "XST": "0xC60D6662027F5797Cf873bFe80BcF048e30Fc35e",
    "MUTE": "0xa49d7499271ae71cd8ab9ac515e6694c755d400c",
    "MTRG": "0xbd2949f67dcdc549c6ebe98696449fa79d988a9f",
}
API_USER_NAME = ""
API_TOKEN = ""
URL = f"https://public-api.arken.finance/insider/v1/{str(CHAIN_ID)}/tokens/price?addresses="
HEADERS = {
    "X-API-Username": API_USER_NAME,
    "X-API-Token": API_TOKEN,
}


def get_prices_from_addrs(addrs):
    r = requests.get(URL + ",".join(addrs), headers=HEADERS)
    r.raise_for_status()

    return r.json()


def get_price_map(symbols):
    addrs = set()
    for symbol in symbols:
        if symbol == "ETH":
            symbol = "WETH"
        if symbol in SYMBOLS_TO_ADDRS:
            addrs.add(SYMBOLS_TO_ADDRS[symbol])

    prices = get_prices_from_addrs(list(addrs))

    addrs_to_symbols = {v.lower(): k for k, v in SYMBOLS_TO_ADDRS.items()}

    price_map = defaultdict(lambda: "-")
    for addr, data in prices.items():
        addr = addr.lower()
        if addr in addrs_to_symbols:
            symbol = addrs_to_symbols[addr]
            price = Decimal(data["price"])
            if price < 0:
                raise Exception("Negative number returned")

            price_map[symbol] = "{:.9f}".format(price).rstrip("0").rstrip(".")

    if "ETH" in symbols:
        price_map["ETH"] = price_map["WETH"]

    return price_map


def main(symbols):
    price_map = get_price_map(symbols)
    return ",".join([price_map[symbol] for symbol in symbols])


if __name__ == "__main__":
    try:
        print(main(sys.argv[1:]))
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
