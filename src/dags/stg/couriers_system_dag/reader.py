from datetime import datetime
from typing import Dict, List

import requests


def make_request_str(url: str, params: Dict):

    request_str = url + '?'
    for (key, value) in params.items():
        request_str += str(key) + '=' + str(value) + '&'

    return request_str.replace(' ', '%20')[:-1]


class Reader:
    def __init__(self, url: str, headers: Dict, params: Dict) -> None:
        self.url = url
        self.headers = headers
        self.params = params

    def get_data(self) -> List[Dict]:

        result = []
        request_str = make_request_str(self.url, self.params)

        data = requests.get(
            request_str,
            headers=self.headers
        ).json()

        while (data):
            result += data

            if (("offset" in self.params) and ("limit" in self.params)):
                self.params["offset"] += self.params["limit"]
            else:
                break

            request_str = make_request_str(self.url, self.params)
            data = requests.get(
                request_str,
                headers=self.headers
            ).json()

        return result
