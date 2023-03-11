"""Simple HTTP get wrapper that stores the history of fetched urls."""

import requests
import sqlite3
import lzma
import hashlib
import datetime
import urllib3
import time

# import logging

# from http.client import HTTPConnection
# HTTPConnection.debuglevel = 1

# logging.basicConfig() # you need to initialize logging, otherwise you will not see anything from requests
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/110.0'}

__version__ = "0.0.1"

class HistorySession:

    def __init__(self, filename="requests-history.db"):
        self.db = sqlite3.connect(filename)

        cur = self.db.cursor()
        try:
            cur.execute("CREATE TABLE pages (url text, sha256 blob, content_xz blob, first_fetch timestamp, last_fetch timestamp)")
        except sqlite3.OperationalError:
            pass

        self.requests = requests.Session()
        retry_policy = urllib3.Retry(total=10, backoff_factor=0.1)
        a = requests.adapters.HTTPAdapter(max_retries=retry_policy)
        self.requests.mount('https://', a)

    def get(self, url, fetch_again=False, crawl_delay=0, only_after=None):
        now = datetime.datetime.now()
        cur = self.db.cursor()
        cur.execute("SELECT sha256, content_xz, first_fetch, last_fetch FROM pages WHERE url = ? ORDER BY last_fetch DESC LIMIT 1", (url,))
        past_fetch = cur.fetchone()
        if past_fetch is None or fetch_again:
            if only_after:
                headers["If-Modified-Since"] = only_after.strftime("%a, %d %b %Y %H:%M:%S GMT")
            elif "If-Modified-Since" in headers:
                del headers["If-Modified-Since"]
            before = time.monotonic()
            response = self.requests.get(url, headers=headers, timeout=30)
            after = time.monotonic()
            diff = after - before
            if diff > 1:
                print("Slow request:", diff)
            if response.status_code == 304:
                return None
            content = response.content
            sha256 = hashlib.sha256(content).digest()
            if past_fetch and sha256 == past_fetch[0]:
                cur.execute("UPDATE pages SET last_fetch = ? WHERE url = ? AND last_fetch = ?", (now, url, past_fetch[3]))
            else:
                content_xz = lzma.compress(content)
                cur.execute("INSERT INTO pages (url, sha256, content_xz, first_fetch, last_fetch) VALUES (?, ?, ?, ?, ?)", (url, sha256, content_xz, now, now))
            self.db.commit()
            time.sleep(crawl_delay)
            return content
        return lzma.decompress(past_fetch[1])

if __name__ == "__main__":
    session = HistorySession()
    session.get("https://accesshub.pdc.wa.gov/node/90569")
    session.get("https://accesshub.pdc.wa.gov/node/90569", fetch_again=True)
