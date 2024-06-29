import json
import locale
import pickle
import threading
import tkinter as tk

from src.models import Config, PageScore, SearchResultTable
from src.modules.crawler import Crawler
from src.modules.pagerank import PageRank, adapter
from timeit import default_timer as timer
from src.services.SearchResultService import SearchResultService

locale.setlocale(locale.LC_ALL, 'tr_TR.UTF-8')

MAX_TITLE_LEN = 77
MAX_LINK_LEN = 105
MAX_DESC_LINE_LEN = 100
MAX_DESC_LEN = MAX_DESC_LINE_LEN * 3

def update_search_results(raw_query, ranks, doc_count, cache_hit):
    if cache_hit:
        query = crawler._preprocess_document(raw_query).split(" ")
        ranks, doc_count = pr.get_pageranks(query, top=10)
    search_result_service.upsert_search_result(
        SearchResultTable(
            query=raw_query,
            results=pickle.dumps((ranks, doc_count))
        ),
        commit=True
    )

def add_line_breaks(text, max_length):
    chunks = [text[i:i + max_length] for i in range(0, len(text), max_length)]
    return '-\n'.join(chunks)

def clear_results():
    for widget in results_container.winfo_children():
        widget.destroy()

def on_mouse_wheel(event):
    canvas.yview_scroll(int(-1*(event.delta/120)), "units")

def display_results(ranks: list[PageScore], doc_count: int, final_time):
    result_summary = tk.Label(results_container, text=f"{doc_count} doküman {final_time:.3f} saniyede tarandı", font=("Helvetica", 12, "italic"))
    result_summary.pack(pady=5)
    
    for i, rank in enumerate(ranks):
        bd = 3 if i == 0 else 1
        result_frame = tk.Frame(results_container, bd=bd, relief="solid", padx=10, pady=5)
        result_frame.pack(fill="x", pady=5)
        result_frame.bind("<Button-1>", lambda e, url=rank.document.url: open_url(url))

        title = rank.document.title or rank.document.url
        if title and len(title) > MAX_TITLE_LEN:
            title = title[:MAX_TITLE_LEN] + "..."
        result_title = tk.Label(result_frame, text=title, font=("Helvetica", 14, "bold"))
        result_title.pack(anchor="w")

        url = rank.document.url[:MAX_LINK_LEN]
        if url and len(url) > MAX_LINK_LEN:
            url = url[:MAX_LINK_LEN] + "..."
        result_url = tk.Label(result_frame, text=url, fg="blue", cursor="hand2")
        result_url.pack(anchor="w")

        description = rank.document.description
        # description = str(rank.idf_score)
        if description:
            description = add_line_breaks(description[:MAX_DESC_LEN], MAX_DESC_LINE_LEN)
            result_description = tk.Label(result_frame, text=description, font=("Helvetica", 12), anchor="w", justify="left")
            result_description.pack(anchor="w", fill="both")

def open_url(url):
    import webbrowser
    webbrowser.open(url)

def search(lucky: bool = False):
    clear_results()
    
    raw_query = query_entry.get().encode('utf-8')
    raw_query_processed = crawler._preprocess_document(raw_query)
    query = raw_query_processed.split(" ")
    if not query:
        results_label.config(text="Lütfen geçerli bir arama sorgusu girin.")
        return

    results_label.config(text="")
    results_label.update()

    start = timer()

    cache_hit = False
    try:
        sr_service_results = search_result_service.get_search_result_by_query(raw_query_processed)
        if not sr_service_results:
            raise Exception("Cache miss")
        cache_hit = True
        ranks, doc_count = pickle.loads(sr_service_results.results)
    except:
        ranks, doc_count = pr.get_pageranks(query, top=10)

    end = timer()
    final_time = end - start
    final_time = final_time if final_time >= 0 else 0


    if not ranks:
        results_label.config(text="Sonuç bulunamadı.\n\n")
        return

    try:
        if lucky:
            open_url(ranks[0].document.url)
        display_results(ranks, doc_count, final_time)
    except Exception as e:
        print("ERROR:", e.__class__.__name__, e)
        results_label.config(text="Sonuçları gösterirken bir hata meydana geldi, lütfen tekrar deneyiniz.\n\n")

    threading.Thread(
        target=update_search_results,
        args=(raw_query_processed, ranks, doc_count, cache_hit)
    ).start()

# Load configuration
with open("config.json") as f:
    config = Config(**json.load(f))

# Load services
search_result_service = SearchResultService(adapter)
crawler = Crawler(config.crawler)
pr = PageRank()

# Styling constants
BG_COLOR = "#f0f0f0"  # Light gray
FONT_TITLE = ("Helvetica", 14, "bold")
FONT_NORMAL = ("Helvetica", 12)
PADX = 10
PADY = 5

root = tk.Tk()
root.title("Arama Motoru")
root.geometry("800x500")
root.configure(bg=BG_COLOR)  # Set background color for the root window

frame = tk.Frame(root, bg=BG_COLOR)
frame.pack(padx=PADX, pady=PADY, fill="both", expand=True)

query_label = tk.Label(frame, text="Aranacak sorguyu girin:", font=FONT_NORMAL, bg=BG_COLOR)
query_label.pack(pady=PADY)

query_entry = tk.Entry(frame, width=50, font=FONT_NORMAL)
query_entry.pack(pady=PADY)

# Centered buttons
center_frame = tk.Frame(frame, bg=BG_COLOR)
center_frame.pack(padx=PADX, pady=PADY)

search_button = tk.Button(center_frame, text="Ara", command=search, font=FONT_NORMAL)
search_button.pack(side="left", padx=PADX, pady=PADY)

lucky_button = tk.Button(center_frame, text="Kendimi şanslı hissediyorum", command=lambda: search(lucky=True), font=FONT_NORMAL)
lucky_button.pack(side="left", padx=PADX, pady=PADY)

results_label = tk.Label(frame, text="", font=FONT_NORMAL, bg=BG_COLOR)
results_label.pack(pady=PADY)

# Create a canvas and scrollbar for the results
canvas = tk.Canvas(frame, bg=BG_COLOR)
scrollbar = tk.Scrollbar(frame, orient="vertical", command=canvas.yview)
scrollable_frame = tk.Frame(canvas, bg=BG_COLOR)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")
canvas.bind_all("<MouseWheel>", on_mouse_wheel)

results_container = scrollable_frame

root.mainloop()