import argparse
import threading

def parse_args():
    parser = argparse.ArgumentParser(description="CLI for the search engine.")
    parser.add_argument("--ip", action="store_true", help="Search for IP addresses.")
    parser.add_argument("--url", action="store_true", help="Search for URLs.")
    parser.add_argument("--page", action="store_true", help="Search for pages.")
    parser.add_argument("--all", action="store_true", help="Search for all.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.all: args.ip = args.url = args.page = True
    if args.ip:
        import ip_search
        t1 = threading.Thread(target=ip_search.run)
        t1.daemon = True
        t1.start()
    if args.url:
        import url_frontier_search
        t2 = threading.Thread(target=url_frontier_search.run)
        t2.daemon = True
        t2.start()
    if args.page:
        import page_search
        t3 = threading.Thread(target=page_search.run)
        t3.daemon = True
        t3.start()
    else:
        print("Please specify a search type.")