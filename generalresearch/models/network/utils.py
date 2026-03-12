import requests


def get_source_ip():
    return requests.get("https://icanhazip.com?").text.strip()
