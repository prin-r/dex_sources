#!/usr/bin/env python3

import sys
from collections import defaultdict
from decimal import Decimal
import requests

CHAIN_ID = 56
SYMBOLS_TO_ADDRS = {
    "BETH": "0x250632378e573c6be1ac2f97fcdf00515d0aa91b",
    "PHB": "0x0409633A72D846fc5BBe2f98D88564D35987904D",
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
