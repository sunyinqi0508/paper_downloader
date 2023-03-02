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
import magic

threadLock = threading.Lock()

try:
    os.mkdir('sigmod')
except FileExistsError:
    ...

d = 0
def prep():
    if os.path.exists('repo') and os.path.getsize('repo') > 65536: 
        response = input('old repo already exists, skip? (Yes/no)')
        if (not response.lower().startswith('n')):
            return
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
    global d
    acm_root = 'https://dl.acm.org'
    cookie = ['']
    
    # acm_root = 'http://' # alternative address
    # cookie = ['--load-cookies=cookies.txt']
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
            filepath = f'{dir_name}/{slugify.slugify(title, max_length=1024, lowercase=False, separator = " ") + ".pdf"}'
            if (
                os.path.exists(filepath) and 
                (os.path.getsize(filepath) > 196608 or
                'PDF' in magic.from_file(filepath))
            ): 
                continue
            subprocess.run(['wget', *cookie, f'{acm_root}/doi/pdf/{doi}', '-O', filepath])
            if (
                os.path.exists(filepath) and 
                os.path.getsize(filepath) < 196608 and 
                'PDF' not in magic.from_file(filepath)
            ): 
                os.remove(filepath)
                continue
            print(f'Downloaded {i}/{len(repo)}')
            d += 1
            time.sleep(30)
        if(amount > 0):
            cp.seek(0)
            cp.truncate()
            cp.write(str(progress + amount))
            return True
        return False

def cleanup():
    root = 'sigmod/'
    fs = [root + rfs for rfs in os.listdir(root) if os.path.isdir(rfs)]
    files = [f + '/' + k for f in fs if os.path.isdir(f) for k in os.listdir(f)]
    pdfs = [ff for ff in files if ff.endswith('.pdf') and os.path.exists(ff) and 'PDF' in magic.from_file(ff)]
    delta = [f for f in files if f not in pdfs]
    md = [(magic.from_file(m), m) for m in delta if os.path.exists(m)]
    if (md):
        for m, d in md:
            print(f'{d}: {m}')
        response = input('Delete all above files? (Yes/no)')
        if (not response.lower().startswith('n')):
            for _, d in md:
                os.remove(d)

def monitor():
    fs = os.listdir('.')
    repo = pickle.loads(open('repo', 'rb').read())

    d = {}
    for r in repo:
        if r[0] not in d:
            d[r[0]] = []
        else:
            d[r[0]].append(r[1])
    while(True):
        files = [f + '/' + k for f in fs if os.path.isdir(f) for k in os.listdir(f)]
        print(len( [ff for ff in files if ff.endswith('.pdf') and os.path.exists(ff) and 'PDF' in magic.from_file(ff)]))
        time.sleep(60)
        res = [(len(os.listdir(f)), f) for f in os.listdir() if os.path.isdir(f)]
        lens = { k[-2:]:len(v) for k, v in d.items()}
        rres = [((lens[c[-2:]] - y)/lens[c[-2:]], y, c[-2:]) for y, c in res]
        rres.sort()
        print(rres)
        time.sleep(45)


batch_size = 45 

prep()
lastd = 0
while(work(batch_size)):
    print(f'{d} papers downloaded in current session.')
    if d - lastd < batch_size:
        continue
    d = lastd
    time.sleep(3600) 
cleanup()
