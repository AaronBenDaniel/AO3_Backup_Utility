from AO3 import Session, Work
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ebooklib import epub, ITEM_DOCUMENT
from os import environ, remove
from pathlib import Path
from re import search, sub
from math import ceil
from tqdm import tqdm
import warnings
from eliot import start_action, to_file, Message
import threading

path = Path(__file__).parent.resolve()
warnings.filterwarnings("ignore")
logging_path = path / "log.debug"
to_file(open(logging_path, "w"))

load_dotenv()


def thread_exception_handler(args):
    args.thread.action.finish(args.exc_value)


# Set global thread exception handler
threading.excepthook = thread_exception_handler


def ascii_only(string: str):
    string = string.replace(" ", "_")
    return sub(
        r"[^qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890\-\_)(`~.><\[\]{}]",
        "",
        string,
    )


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
    with start_action(action_type="Log In", username=username, password=password):
        print("Initializing session")
        session = Session(username, password)
        with open(path / "loginPage.debug", "w", encoding="utf-8") as file:
            file.write(str(session.loginPage))
        print(f"Logged in as {session.username}")
        Message.log(username=f"{session.username}")

    # Get subs list
    with start_action(action_type="Get Subs"):
        print("Retrieving subscriptions")
        subs = session.get_subscriptions(use_threading=True)

    # Remove all non-works
    works = [sub for sub in subs if isinstance(sub, Work)]

    # Load metadata for works (threaded)
    # Batches threads to avoid ratelimits
    n = 10
    with tqdm(total=len(works), desc="Reloading Works") as pbar:
        for i in range(0, len(works), n):
            with start_action(
                action_type="Reload Works Batch",
                data=f"{ceil(i/n)+1}/{ceil(len(works)/n)}",
            ):
                works_to_reload = works[i : i + n]

                threads = []
                for work in works_to_reload:
                    work.set_session(session)
                    thread = work.reload(threaded=True, load_chapters=False)
                    thread.action = start_action(
                        action_type="Reload Work",
                        work_id=work.id,
                    )
                    threads.append(thread)
                for thread in threads:
                    thread.join()
                    pbar.update(1)
                    thread.action.finish()

    # Remove works that do not need to be downloaded (word-count and modify-date unchanged)
    with tqdm(total=len(works), desc="Parsing Works") as pbar:
        works_to_download = []
        for work in works:
            action = start_action(action_type="Parse Work", work_id=work.id)

            fandom = ascii_only(work.fandoms[0]) if work.fandoms else "Other"
            series = work.series[0] if work.series else None

            work_path = output_directory / fandom

            if series:
                work_path = work_path / ascii_only(series.name)

            work_path = work_path / (ascii_only(work.title).replace(" ", "_") + ".epub")

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
                remove(work_path)
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
                remove(work_path)
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
        for i in range(0, len(works_to_download), n):
            with start_action(
                action_type="Download Works Batch",
                data=f"{ceil(i/n)+1}/{ceil(len(works_to_download)/n)}",
            ):
                works = works_to_download[i : i + n]
                threads = []
                for work in works:

                    pbar.set_postfix_str(work.title)

                    fandom = ascii_only(work.fandoms[0]) if work.fandoms else "Other"
                    series = work.series[0] if work.series else None

                    work_path = output_directory / fandom

                    if series:
                        work_path = work_path / ascii_only(series.name)

                    work_path = work_path / (
                        ascii_only(work.title).replace(" ", "_") + ".epub"
                    )

                    # Make parent directories
                    work_path.parent.mkdir(parents=True, exist_ok=True)

                    work.set_session(session)
                    threads.append(
                        [
                            work.download_to_file(work_path, "EPUB", threaded=True),
                            start_action(
                                action_type="Download Work",
                                work_id=work.id,
                                work_path=work_path,
                            ),
                        ]
                    )
                for thread, action in threads:
                    thread.join()
                    pbar.update(1)
                    action.finish()
