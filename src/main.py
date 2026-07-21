from AO3 import Session, Work
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ebooklib import epub, ITEM_DOCUMENT
from os import environ
from pathlib import Path
from tqdm import tqdm
from shutil import rmtree, move
import warnings
from eliot import to_file, Message, start_task
import re
from time import sleep

path = Path(__file__).parent.parent.resolve()
warnings.filterwarnings("ignore")
logging_path = path / "log.debug"
to_file(open(logging_path, "w"))

load_dotenv(override=True)


def ascii_only(string: str):
    string = str(string).replace(" ", "_")
    return re.sub(
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
    rmtree(path / "temp", ignore_errors=True)

    username = environ.get("USERNAME")
    password = environ.get("PASSWORD")
    out = environ.get("OUTPUT_DIRECTORY")
    if not all([username, password, out]):
        print("Missing environment parameter")
        exit()
    output_directory = Path(out)

    Message.log(output_directory=output_directory, logging_path=logging_path)
    print(f"Output Directory: {output_directory}\nLogging Path: {logging_path}")

    if not output_directory.exists() or not output_directory.is_dir():
        Message.log(error="Output path does not exist or is not a directory")
        print("Output path does not exist or is not a directory")
        exit()

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
        subs = session.get_subscriptions()
        if getattr(session, "exceptions", None):
            Message.log(num_sub_page_failures=str(len(session.exceptions)))

    # Remove all non-works and duplicates
    works = []
    for item in subs:
        if isinstance(item, Work) and item not in works:
            works.append(item)

    failures = []

    with tqdm(total=len(works), desc="Reloading Works") as pbar:
        for work in works:

            work.set_session(session)
            action = start_task(
                action_type="Reload Work",
                work_id=work.id,
            )
            try:
                work.reload(load_chapters=False)
            except Exception as e:
                failures.append(work.id)
                Message.log(
                    task_uuid=action._identification["task_uuid"],
                    action_type=action._identification["action_type"],
                    outcome=str(e),
                    work_id=work.id,
                )

            action.finish()
            pbar.update(1)

            sleep(1)

    # Remove works that do not need to be downloaded (word-count unchanged)
    with tqdm(total=len(works), desc="Parsing Works") as pbar:
        Path(path / "temp").mkdir(exist_ok=True)
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
                    re.search(r"Words:\s*([\d,]+)", metadata).group(1).replace(",", "")
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

            except:
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

            pbar.update(1)
            Message.log(
                task_uuid=action._identification["task_uuid"],
                action_type=action._identification["action_type"],
                outcome="EPUB up to date",
                work_path=work_path,
                work_id=work.id,
            )
            action.finish()

    with tqdm(total=len(works_to_download), desc="Downloading Works") as pbar:
        for work in works_to_download:
            if work.id in failures:
                continue

            pbar.set_postfix_str(work.title)

            work_path = get_path(work)

            # Download works to temp directory
            work.set_session(session)
            action = start_task(
                action_type="Download Work",
                work_id=work.id,
                work_path=work_path,
            )
            try:
                work.download_to_file(
                    path / "temp" / (str(work.id) + ".tmp"),
                    "EPUB",
                )
            except Exception as e:
                failures.append(work.id)
                Message.log(
                    task_uuid=action._identification["task_uuid"],
                    action_type=action._identification["action_type"],
                    outcome=str(e),
                    work_id=work.id,
                )

            action.finish()
            pbar.update(1)

            sleep(1)

        # Move works from temp directory to output directory
        for work in works_to_download:
            if work.id in failures:
                continue
            work_path = get_path(work)
            work_path.parent.mkdir(parents=True, exist_ok=True)
            move(path / "temp" / (str(work.id) + ".tmp"), work_path)

    rmtree(path / "temp", ignore_errors=True)
    print(f"Completed with {len(failures)} failures")
    Message.log(num_failures=len(failures), failures=failures)
