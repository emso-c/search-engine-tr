from urllib.parse import urlparse

from src.database.adapter import load_db_adapter
from src.services import BacklinkService, IPService

adapter = load_db_adapter()
backlink_service = BacklinkService(adapter)
ip_service = IPService(adapter)


def _get_base_url(url: str) -> str:
    parsed_uri = urlparse(url)
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri).strip()
    return result

def _is_same_domain(url1: str, url2: str) -> bool:
    return _get_base_url(url1) == _get_base_url(url2)

def _is_same_subbdomain(url1: str, url2: str) -> bool:
    parsed_uri1 = urlparse(url1)
    parsed_uri2 = urlparse(url2)
    
    domain1 = parsed_uri1.netloc.split(".")
    domain2 = parsed_uri2.netloc.split(".")
    
    return domain1[-2:] == domain2[-2:]
    

print("Initial backlink count:", backlink_service.count())


ip_service.remove_duplicates()

# clear the scores before updating to
# avoid adding to existing scores
for ip_obj in ip_service.get_ips():
    ip_obj.score = 0
    ip_service.update_ip(ip_obj)

# update ip scores based on backlinks
for backlink in backlink_service.get_backlinks():
    if _is_same_domain(backlink.source_url, backlink.target_url):
        continue
    if _is_same_subbdomain(backlink.source_url, backlink.target_url):
        continue

    base_source = _get_base_url(backlink.source_url)
    base_target = _get_base_url(backlink.target_url)
    
    ip_target_obj = ip_service.get_ip_by_domain(base_target)
    if not ip_target_obj:
        print("No IP found for target:", base_target)
        continue
    
    ip_target_obj.score += 1
    ip_service.update_ip(ip_target_obj)
    print(f"Found backlink from {backlink.source_url} to {base_target}")

print("Committing changes...")
ip_service.commit(verbose=False)

print("Backlink analysis complete. Total backlinks:", backlink_service.count())