import threading

_original_run = threading.Thread.run


def _patched_run(self):
    self.exception = None
    try:
        _original_run(self)
    except Exception as e:
        self.exception = e


threading.Thread.run = _patched_run

from AO3 import Session, Work
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ebooklib import epub, ITEM_DOCUMENT
from os import environ, replace, removedirs
from pathlib import Path
from re import search, sub
from math import ceil
from tqdm import tqdm
import warnings
from eliot import to_file, Message, start_task
import threading

path = Path(__file__).parent.parent.resolve()
warnings.filterwarnings("ignore")
logging_path = path / "log.debug"
to_file(open(logging_path, "w"))

load_dotenv()


def ascii_only(string: str):
    string = string.replace(" ", "_")
    return sub(
        r"[^qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890\-\_)(`~.><\[\]{}]",
        "",
        string,
    )


def get_path(work: Work):
    fandom = ascii_only(work.fandoms[0]) if work.fandoms else "Other"
    series = work.series[0] if work.series else None

    work_path = output_directory / fandom

    if series:
        work_path = work_path / ascii_only(series.name)

    work_path = work_path / (ascii_only(work.title).replace(" ", "_") + ".epub")

    return work_path


if __name__ == "__main__":
    try:
        username = environ.get("USERNAME")
        password = environ.get("PASSWORD")
        output_directory = Path(environ.get("OUTPUT_DIRECTORY"))
    except TypeError:
        print("Missing environment parameter")
        exit()

    Message.log(output_directory=output_directory, logging_path=logging_path)
    print(f"Output Directory: {output_directory}\nLogging Path: {logging_path}")

    # Log in
    with start_task(action_type="Log In", username=username, password=password):
        print("Initializing session")
        session = Session(username, password)
        with open(path / "loginPage.debug", "w", encoding="utf-8") as file:
            file.write(str(session.loginPage))
        print(f"Logged in as {session.username}")
        Message.log(username=f"{session.username}")

    # Get subs list
    with start_task(action_type="Get Subs"):
        print("Retrieving subscriptions")
        subs = session.get_subscriptions(use_threading=True)
        if getattr(session,"exceptions",None):
            Message.log(num_sub_page_failures=str(len(session.exceptions)))

    # Remove all non-works
    works = [sub for sub in subs if isinstance(sub, Work)]

    failures = []

    # Load metadata for works (threaded)
    # Batches threads to avoid ratelimits
    n = 10
    with tqdm(total=len(works), desc="Reloading Works") as pbar:
        for i in range(0, len(works), n):
            with start_task(
                action_type="Reload Works Batch",
                batch=f"{ceil(i/n)+1}/{ceil(len(works)/n)}",
            ) as parent:
                works_to_reload = works[i : i + n]

                threads = []
                for work in works_to_reload:
                    if work.id in failures:
                        continue

                    work.set_session(session)
                    thread = work.reload(threaded=True, load_chapters=False)
                    thread.action = start_task(
                        action_type="Reload Work",
                        work_id=work.id,
                        parent=parent.task_uuid,
                    )
                    thread.work_id = work.id
                    threads.append(thread)
                for thread in threads:
                    thread.join()
                    pbar.update(1)
                    thread.action.finish(exception=thread.exception)
                    if thread.exception:
                        failures.append(thread.work_id)

    # Remove works that do not need to be downloaded (word-count and modify-date unchanged)
    with tqdm(total=len(works), desc="Parsing Works") as pbar:
        works_to_download = []
        for work in works:
            if work.id in failures:
                continue

            action = start_task(action_type="Parse Work", work_id=work.id)

            work_path = get_path(work)
            if not work_path.exists():
                works_to_download.append(work)
                pbar.update(1)
                Message.log(
                    task_uuid=action._identification["task_uuid"],
                    action_type=action._identification["action_type"],
                    outcome="Path does not exist",
                    work_path=work_path,
                    work_id=work.id,
                )
                action.finish()
                continue

            # Open existing .epub file
            try:
                epub_file = epub.read_epub(work_path)
            except epub.EpubException:
                works_to_download.append(work)
                pbar.update(1)
                Message.log(
                    task_uuid=action._identification["task_uuid"],
                    action_type=action._identification["action_type"],
                    outcome="Invalid EPUB",
                    work_path=work_path,
                    work_id=work.id,
                )
                action.finish()
                continue

            # Extract all chapters
            chapters = [
                chapter for chapter in epub_file.get_items_of_type(ITEM_DOCUMENT)
            ]

            # Parse the first chapter (Always the Preface)
            soup = BeautifulSoup(chapters[0].get_body_content(), features="lxml")

            # Extract important chunk of metadata
            metadata = str(soup.find_all("dd")[-1])

            # Extract word count
            epub_wc = int(
                search(r"Words:\s*([\d,]+)", metadata).group(1).replace(",", "")
            )

            ao3_wc = work.words

            if epub_wc != ao3_wc:
                works_to_download.append(work)
                pbar.update(1)
                Message.log(
                    task_uuid=action._identification["task_uuid"],
                    action_type=action._identification["action_type"],
                    outcome="EPUB out of date",
                    work_path=work_path,
                    work_id=work.id,
                )
                action.finish()
                continue

            pbar.update(1)
            Message.log(
                task_uuid=action._identification["task_uuid"],
                action_type=action._identification["action_type"],
                outcome="EPUB up to date",
                work_path=work_path,
                work_id=work.id,
            )
            action.finish()

    # Download works (threaded)
    # Batches threads to avoid ratelimits
    n = 10
    with tqdm(total=len(works_to_download), desc="Downloading Works") as pbar:
        Path(path / "temp").mkdir(exist_ok=True)
        for i in range(0, len(works_to_download), n):
            with start_task(
                action_type="Download Works Batch",
                batch=f"{ceil(i/n)+1}/{ceil(len(works_to_download)/n)}",
            ) as parent:
                works = works_to_download[i : i + n]
                threads = []
                for work in works:
                    if work.id in failures:
                        continue

                    pbar.set_postfix_str(work.title)

                    work_path = get_path(work)

                    # Download works to temp directory
                    work.set_session(session)
                    thread = work.download_to_file(
                        path / "temp" / (str(work.id) + ".tmp"),
                        "EPUB",
                        threaded=True,
                    )
                    thread.action = start_task(
                        action_type="Download Work",
                        work_id=work.id,
                        work_path=work_path,
                        parent=parent.task_uuid,
                    )
                    thread.work_id = work.id
                    threads.append(thread)
                for thread in threads:
                    thread.join()
                    pbar.update(1)
                    thread.action.finish(exception=thread.exception)
                    if thread.exception:
                        failures.append(thread.work_id)

        # Move works from temp directory to output directory
        for work in works_to_download:
            if work.id in failures:
                continue
            work_path = get_path(work)
            work_path.parent.mkdir(parents=True, exist_ok=True)
            replace(path / "temp" / (str(work.id) + ".tmp"), work_path)

    removedirs(path / "temp")
    print(f"Completed with {len(failures)} failures")
    Message.log(num_failures=len(failures), failures=failures)
