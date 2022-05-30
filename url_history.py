import requests
import sqlite3
import lzma
import hashlib
import datetime

class HistorySession:

    def __init__(self, filename="requests-history.db"):
        self.db = sqlite3.connect(filename)

        cur = self.db.cursor()
        try:
            cur.execute("CREATE TABLE pages (url text, sha256 blob, content_xz blob, first_fetch timestamp, last_fetch timestamp)")
        except sqlite3.OperationalError:
            pass

    def get(self, url, fetch_again=False):
        now = datetime.datetime.now()
        cur = self.db.cursor()
        cur.execute("SELECT sha256, content_xz, first_fetch, last_fetch FROM pages WHERE url = ? ORDER BY last_fetch DESC LIMIT 1", (url,))
        past_fetch = cur.fetchone()
        if past_fetch is None or fetch_again:
            response = requests.get(url)
            content = response.content
            sha256 = hashlib.sha256(content).digest()
            if past_fetch and sha256 == past_fetch[0]:
                cur.execute("UPDATE pages SET last_fetch = ? WHERE url = ? AND last_fetch = ?", (now, url, past_fetch[3]))
            else:
                content_xz = lzma.compress(content)
                cur.execute("INSERT INTO pages (url, sha256, content_xz, first_fetch, last_fetch) VALUES (?, ?, ?, ?, ?)", (url, sha256, content_xz, now, now))
            return content
        return lzma.decompress(past_fetch[1])

if __name__ == "__main__":
    session = HistorySession()
    session.get("https://accesshub.pdc.wa.gov/node/90569")
    session.get("https://accesshub.pdc.wa.gov/node/90569", fetch_again=True)
