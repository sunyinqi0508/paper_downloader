# Bill Sun 2023

import os
import re
import subprocess
import crossref_commons.retrieval
import pickle
import time
import slugify
import threading
import concurrent.futures

threadLock = threading.Lock()

try:
    os.mkdir('sigmod')
except FileExistsError:
    ...

def prep():
    repo = []
    mod = 'https://dl.acm.org/conference/mod/proceedings'
    subprocess.run(['wget', mod, '-O', 'MOD'])
    sigmods = open('MOD', 'r').read()
    pdois = re.findall(r'<a href="/doi/proceedings/(\d*\.\d*/\d*)">SIGMOD.*?(\d*):.*?</a>', sigmods)
    def get_pdoi(pdoi):
        dir_name = f'sigmod/sigmod{pdoi[1]}'
        try:
            os.mkdir(dir_name)
        except FileExistsError:
            ...
        subprocess.run(['wget', f'https://dl.acm.org/doi/proceedings/{pdoi[0]}', '-O', pdoi[1]])
        sigmod = open(pdoi[1], 'r').read()
        dois = set(re.findall(rf'{pdoi[0]}\.\d*', sigmod))
        
        for doi in dois:
            title = doi.replace('/', '_') 
            retries = 0
            while retries < 10:
                try:
                    _title = crossref_commons.retrieval.get_publication_as_json(doi)['title'][0]
                    if type(_title) is str:
                        title = _title 
                except BaseException as e:
                    retries += 1
                    print(e)
                    continue
                break

            print(dir_name, doi, title)
            with threadLock:
                repo.append((dir_name, doi, title))
    with concurrent.futures.ThreadPoolExecutor(max_workers=48) as executor:
        executor.map(get_pdoi, pdois)
    
    pickle.dump(repo, open('repo', 'wb'))
    open('repo.txt', 'w').write('\n'.join([' '.join(r) for r in repo]))
        
def work(amount):
    repo = pickle.load(open('repo', 'rb'))
    if not os.path.exists('checkpoint'):
        with open('checkpoint', 'w') as cp:
            cp.write('0')
    with open('checkpoint', 'r+') as cp:
        progress = 0
        try:
            progress = int(cp.read())
        except (ValueError, IOError):
            ...
        if progress + amount > len(repo):
            amount = len(repo) - progress
        for i in range(progress, progress + amount):
            dir_name, doi, title = repo[i]
            filepath = f'{dir_name}/{slugify.slugify(title) + ".pdf"}'
            if os.path.exists(filepath) and os.path.getsize(filepath) > 32768: 
                continue
            subprocess.run(['wget', f'https://dl.acm.org/doi/pdf/{doi}', '-O', filepath])
            time.sleep(30)
        if(amount > 0):
            cp.write(str(progress + amount))
            return True
        return False

batch_size = 45 # around 30 papers/hr
prep()
while(work(batch_size)):
    time.sleep(3600) 
