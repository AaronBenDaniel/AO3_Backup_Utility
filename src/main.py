from AO3 import Session, Work
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ebooklib import epub, ITEM_DOCUMENT
from os import environ, remove
from pathlib import Path
from re import search, sub
from math import ceil

load_dotenv()
output_directory = Path(environ.get("OUTPUT_DIRECTORY"))


def ascii_only(string: str):
    string = string.replace(" ", "_")
    return sub(
        r"[^qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890\-\_)(`~.><\[\]{}]",
        "",
        string,
    )


print("Initializing session")
session = Session(environ.get("USERNAME"), environ.get("PASSWORD"))

# Get subs list
subs = session.get_subscriptions(use_threading=True)

# Remove all non-works
works = [sub for sub in subs if isinstance(sub, Work)]

# Load metadata for works (threaded)
# Batches threads to avoid ratelimits
n = 10
for i in range(0, len(works), n):
    print(f"Reloading batch: {ceil(i/n)+1}/{ceil(len(works)/n)}")

    works_to_reload = works[i : i + n]

    threads = []
    for work in works_to_reload:
        work.set_session(session)
        threads.append(work.reload(threaded=True, load_chapters=False))
    for thread in threads:
        thread.join()


# Remove works that do not need to be downloaded (word-count and modify-date unchanged)
print("Parsing works")
works_to_download = []
for work in works:

    fandom = ascii_only(work.fandoms[0]) if work.fandoms else "Other"
    series = work.series[0] if work.series else None

    work_path = output_directory / fandom

    if series:
        work_path = work_path / ascii_only(series.name)

    work_path = work_path / (ascii_only(work.title).replace(" ", "_") + ".epub")

    if not work_path.exists():
        print(f"Path does not exist: {work_path}")
        works_to_download.append(work)
        continue

    # Open existing .epub file
    try:
        epub_file = epub.read_epub(work_path)
    except epub.EpubException:
        remove(work_path)
        works_to_download.append(work)
        continue

    # Extract all chapters
    chapters = [chapter for chapter in epub_file.get_items_of_type(ITEM_DOCUMENT)]

    # Parse the first chapter (Always the Preface)
    soup = BeautifulSoup(chapters[0].get_body_content(), features="lxml")

    # Extract important chunk of metadata
    metadata = str(soup.find_all("dd")[-1])

    # Extract word count
    epub_wc = int(search(r"Words:\s*([\d,]+)", metadata).group(1).replace(",", ""))

    ao3_wc = work.words

    if epub_wc != ao3_wc:
        print(f"EPUB out of date: {work_path}")
        remove(work_path)
        works_to_download.append(work)
        continue
    

# Download works (threaded)
# Batches threads to avoid ratelimits
n = 10
for i in range(0, len(works_to_download), n):
    print(f"Downloading batch: {ceil(i/n)+1}/{ceil(len(works_to_download)/n)}")

    works = works_to_download[i : i + n]
    threads = []
    for work in works:

        print(f"Downloading: {work.title}")

        fandom = ascii_only(work.fandoms[0]) if work.fandoms else "Other"
        series = work.series[0] if work.series else None

        work_path = output_directory / fandom

        if series:
            work_path = work_path / ascii_only(series.name)

        work_path = work_path / (ascii_only(work.title).replace(" ", "_") + ".epub")

        # Make parent directories
        work_path.parent.mkdir(parents=True, exist_ok=True)

        work.set_session(session)
        threads.append(work.download_to_file(work_path, "EPUB", threaded=True))
    for thread in threads:
        thread.join()
