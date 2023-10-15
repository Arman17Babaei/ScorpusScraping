import json
import requests
from typing import Tuple, Any, Iterator, Dict
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import numpy as np
import argparse
import pickle

API_KEY = "4ce20302fffc6c64e549d2a3580afe5e"

search_term = "ALL(learn) AND TITLE-ABS-KEY(auditing)"

cache_dir = Path("cache")
cache_dir.mkdir(exist_ok=True)

memory_cache = {}
def cached_get(*args, **kwargs):
    params = tuple(args) + tuple(
        (key, tuple(value) if type(value) is list else value)
        for key, value in kwargs.items()
    )
    if params in memory_cache:
        return memory_cache[params]

    request_path = cache_dir.joinpath(f"{hash(params)}")
    if request_path.exists():
        with request_path.open() as req_file:
            try:
                resp = pickle.load(req_file)
                memory_cache[params] = resp
                return resp
            except:
                print("Couldn't load request ", params)

    resp = requests.get(*args, **kwargs)
    with request_path.open("wb") as req_file:
        pickle.dump(resp, req_file)
    memory_cache[params] = resp
    return resp


def extract_headers(
    api_key: str, search_term: str, params: Iterator[Tuple[str, Any]] = ()
):
    params = {key: value for key, value in params}
    params.update(
        {
            "apiKey": api_key,
            "query": search_term,
            "httpAccept": "application/json",
            "subj": "BUSI",
            "date": "2002-2022",
        }
    )

    def get(start=0, count=25):
        params.update({"start": start, "count": count})
        s = cached_get(
            "https://api.elsevier.com/content/search/scopus",
            params=[(key, value) for key, value in params.items()],
        )
        if s.status_code != 200:
            raise Exception(f"Got response with code {s.status_code}: {s.content}")
        return s.json()

    data = []
    print("Getting headers from Elsevier...")
    page = get()
    total_count = int(page["search-results"]["opensearch:totalResults"])
    print(f"{total_count} in total")
    step = 25
    for cur_item in tqdm(range(0, total_count, step)):
        items = page["search-results"]["entry"]
        data += items
        if cur_item + step < total_count:
            page = get(
                start=cur_item + step, count=min(step, total_count - cur_item - step)
            )
    return data


def get_data_frame(items: Iterator[Dict[Any, Any]], fields: Iterator[Any]):
    cols = {field if type(field) is str else field.__name__: [] for field in fields}
    print("Getting data fields...")
    for item in tqdm(items):
        for field in fields:
            field_name = field
            if callable(field):
                value = field(item)
                field_name = field.__name__
            elif type(field) is str and field in item:
                value = item[field]
            else:
                value = pd.NA
            cols[field_name].append(value)
    return pd.DataFrame(data=cols)


def authors(item):
    if "prism:doi" not in item:
        return pd.NA
    doi = item["prism:doi"]
    resp = cached_get(f"https://api.crossref.org/works/{doi}")
    if resp.status_code != 200:
        print(
            f"Couldn't get metadata of doi {doi} "
            f"with response {resp.status_code}: {resp.content}"
        )
        return pd.NA

    jsn = resp.json()
    if "author" not in jsn["message"]:
        return pd.NA
    authors = jsn["message"]["author"]

    def get_name(author):
        if "family" in author and "given" in author:
            return author["given"] + " " + author["family"]
        if "family" in author and "name" in author:
            return author["name"] + " " + author["family"]
        if "family" in author:
            return author["family"]
        if "name" in author:
            return author["name"]
        if "given" in author:
            return author["given"]
        print(author)
        return str(author)

    return "|".join(map(get_name, authors))


def abstract(item):
    if "prism:doi" not in item:
        return pd.NA
    doi = item["prism:doi"]
    resp = cached_get(f"https://api.crossref.org/works/{doi}")
    if resp.status_code != 200:
        print(
            f"Couldn't get metadata of doi {doi} "
            f"with response {resp.status_code}: {resp.content}"
        )
        return pd.NA

    jsn = resp.json()
    if "abstract" in jsn["message"]:
        return jsn["message"]["abstract"]

    return pd.NA


def main():
    headers_path = Path("headers.json")
    if not headers_path.exists() or args.update_headers:
        headers = extract_headers(API_KEY, search_term)
        with headers_path.open("w") as f:
            json.dump(headers, f)
    else:
        with headers_path.open() as f:
            headers = json.load(f)

    fields = [
        "dc:title",
        "prism:publicationName",
        "prism:volume",
        "prism:coverDate",
        "subtypeDescription",
        "citedby-count",
        "prism:doi",
        "dc:creator",
        authors,
        abstract,
    ]

    df = get_data_frame(headers, fields)
    df.to_csv("result.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update_headers", action="store_true", default=False)
    args = parser.parse_args()
    main()
