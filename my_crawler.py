#------coding:utf-8------
import urllib.request, urllib
import re
from urllib import parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import ZODB, ZODB.FileStorage 
import BTrees.OOBTree
import transaction
import argparse

# Позволяет исключить не ссылки (например, файлы или код javascript)
extensions = "json|3dm|3ds|dwg|dxf|max|obj|7z|cbr|deb|gz|pkg|rar|rpm|sitx|tar|zip|zipx|aif|iff|m3u|m4a|mid|mp3|mpa|ra|wav|wma|ai|eps|ps|svg|3g2|3gp|asf|asx|avi|flv|m4v|mov|mp4|mpg|rm|srt|swf|vob|wmv|tmp|gpx|kml|kmz|doc|docx|ibooks|indd|key|odt|pages|pct|pdf|pps|ppt|pptx|rtf|tex|wpd|wps|xlr|xls|xlsx|ics|part|hqx|keychain|mim|uue|dem|gam|nes|rom|sav|apk|app|bat|cgi|com|exe|gadget|jar|msi|pif|vb|wsf|cfg|cue|ini|prf|bin|dmg|iso|mdf|toast|vcd|crx|plugin|bmp|dds|gif|jpg|png|psd|pspimage|tga|thm|tif|tiff|yuv|cab|cpl|cur|deskthemepack|dll|dmp|drv|icns|ico|lnk|sys|c|class|cpp|cs|dtd|fla|h|java|lua|m|pl|py|sh|sln|vcxproj|xcodeproj|log|txt|accdb|db|dbf|mdb|pdb|sql|bak|tmp|csv|dat|gbr|ged|sdf|xml|fnt|fon|otf|ttff"
not_links = r'mailto|whatsapp|skype|tel|phone|javascript|#|.*\.(?:%s)$' % extensions
p_not_links = re.compile(not_links)
# Находит title страницы
p_title = re.compile(r'(?s)<title[^>]*>(.*)</title>')
# Ищет ссылки
p_links = re.compile(r'''<a [^>]*href=(["'])(.*?)\1''')
# Находит корень текущего URL для корректной сборки ссылки
p_root = re.compile(r'(?:https?:)?//[^/]*')
# Находит текущую директорию для корректной сборки ссылки
p_current = re.compile(r'(.*/)')
p_abs = re.compile(r'https?://')
p_cyrillic = re.compile(r'((?:https?)?://)(%[^/]*)(.*)')

def get_html(url):
    try:
        with urllib.request.urlopen(url) as response: 
            html = response.read()
        return html.decode('utf-8')
    except:
        return ''

def get_title(html):
    m_title = p_title.search(html)
    if m_title:
        return m_title.group(1).strip()
    else:
        return 'No title found'

def get_links(html, url):    
    m_root = p_root.match(url)
    root = m_root.group(0)   
    m_current = p_current.match(url)
    current = m_current.group(0)    
    links = set()    
    for link in set([e[1] for e in p_links.findall(html) if len(e[1]) and not p_not_links.match(e[1])]):         
        m_abs = p_abs.match(link)
        m_cyrillic = p_cyrillic.match(link)
        if link.startswith('//'):
            link = 'http:' + link
        elif link.startswith('/'):
            link = root + link
        elif link.startswith('?'):
            link = url + link
        elif m_cyrillic:
            pref, cyr, rest = m_cyrillic.groups()
            cyr = urllib.parse.unquote(cyr)
            link = pref + cyr.encode('idna').decode('ascii') + rest
        elif not m_abs:
            link = current + link
        links.add(link)        
    return links

def process(url):
    html = get_html(url)
    if html:
        title = get_title(html)
        links = get_links(html, url) 
        return title, links, url#, html
    else:
        return "BAD URL", set(), url

def load(main_url):
    second_links = set()
    with ThreadPoolExecutor(max_workers=5) as pool:
        processed_urls = {main_url : process(main_url)}
        links = processed_urls[main_url][1]        
        results = [pool.submit(process, url) for url in links]        
        for future in as_completed(results):
            title, links, url = future.result()
            processed_urls.update({url : (title, links)})
            second_links.update(links - processed_urls.keys())        
        results = [pool.submit(process, url) for url in second_links]       
        for future in as_completed(results):
            title, links, url = future.result()
            processed_urls.update({url : (title, links)})                        
    return processed_urls

def save_results(processed_urls):
    storage = ZODB.FileStorage.FileStorage('data.fs')
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root

    root.urls = BTrees.OOBTree.BTree()
    root.urls.update(processed_urls)
    transaction.commit()
    db.close()
    
def get_N_urls(main_url, N):
    try:
        storage = ZODB.FileStorage.FileStorage('data.fs')
        db = ZODB.DB(storage)
        connection = db.open()
        root = connection.root
    except:
        print("Cannot 'get' before 'load'")
    if main_url in root.urls:
        l = list(root.urls[main_url][1])
    else:
        print("No such URL in the DB")
    if N > len(l):
        N = len(l)
    for i in range(N):
        url = l[i]
        try:
            title = root.urls[url][0]
        except:
            title = "Not found in DB"
        print(url + ': "' + title + '"')
    db.close()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, help="Commands are 'load' and 'get'")
    parser.add_argument("url", type=str, help="URL that you want to process")
    parser.add_argument("-n", type=int, default = 0, help="Number of links you want to get for the URL")   
    args = parser.parse_args()
    
    if args.command == 'load':
        data = load(args.url)
        save_results(data)
    elif args.command == 'get':
        get_N_urls(args.url, args.n)
    else:
        print('Commands are only "load" and "get"')