# Bill Sun 2023

import os
import urllib.request
import urllib.parse
import urllib.error
import re
import concurrent.futures
from typing import Tuple
import threading

threadLock = threading.Lock()
total = 0
cnt = 0
flock = threading.Lock()
failed = []
try:
    os.mkdir('vldb')
except FileExistsError:
    ...
    
def downloader(i : int):
    i += 1
    try:
        os.mkdir(f'vldb/vol{i}')
    except FileExistsError:
        ...
    req = urllib.request.urlopen(f'http://vldb.org/pvldb/volumes/{i}/')
    web = req.read().decode('utf-8')
    #urls = re.findall(rf'"pdf":"(https?://.*?{i}/.*?.pdf)"', web)
    urls = re.findall(rf'{{(.*?"pdf":"https?://.*?{i}/.*?.pdf".*?)}}', web)
    urls = [(re.findall(r'"title":"(.*?)"', u)[0],re.findall(r'"pdf":"(.*?)"', u)[0]) for u in urls]
    global total, cnt
    with threadLock:
        total += len(urls)
    def get_urls(t : Tuple[str]):
        u = t[1]
        global cnt, total, failed, flock
        print(f'downloading {cnt}/{total} {u}')
        last_slash = u.rfind('/') + 1
        fname = u[last_slash:]
        head = u[:last_slash]
        retries = 0
        while True:
            try:
                with open(f'vldb/vol{i}/{t[0]}.pdf', 'wb') as fp:
                    fp.write(
                        urllib.request.urlopen(
                            head + 
                            urllib.parse.quote(fname)
                        ).read()
                    )
            except (urllib.error.URLError, urllib.error.HTTPError, urllib.error.ContentTooShortError):
                if retries < 10:
                    retries += 1
                    continue
                else:
                    with flock:
                        failed.append(u)
            # except BaseException as e:
            #     print(e)
            break
        with threadLock:
            cnt += 1
        if (cnt % 20 == 0):
            print(f'Progress {100*cnt/total}%')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(get_urls, urls)

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(downloader, range(16))
    
print(f'Progress {100*cnt/total}%')

if len(failed):
    print(f'Failed items ({len(failed)}):')
    err_failed = '\n'.join(failed)
    print(err_failed)
    with open('error.log', 'w') as fp:
        fp.write(err_failed)

