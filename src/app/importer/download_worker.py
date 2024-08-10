import logging
import queue
import random
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Optional, Union

import pydrive2.files
from pydrive2.files import GoogleDriveFile

from ..core import utils
from ..core.gdrive import GoogleDriveClient
from ..core.models.exceptions import DownloadFailedError, OperationCancelledError
from ..log.log_importer import importer as log
from ..models import CancellationToken, CollectionFileChange, CollectionFileQueueItem, StatusFlag
from . import utils as importer_utils


def worker(
    queue_items: Iterable[CollectionFileQueueItem],
    gdrive_client: GoogleDriveClient,
    thread_pool_size: int,
    database_queue: queue.Queue,
    import_queue: queue.Queue,
    status_flag: StatusFlag,
    worker_timeout: float,
    worker_timed_out_flag: StatusFlag,
    cancel_token: CancellationToken,
    debug_mode: bool,
    parent_logger: logging.Logger,
):
    status_flag.value = True
    worker_timed_out_flag.value = False

    parent_logger.info("Download worker started...")
    logger = parent_logger.getChild("downloadWorker")

    logger.debug("Setting up thread pool for downloads with %i workers.", thread_pool_size)
    executor = ThreadPoolExecutor(thread_pool_size, thread_name_prefix="Download gdrive file")
    futures: list[Future] = setup_futures(executor, queue_items, gdrive_client, debug_mode, logger, cancel_token=cancel_token)

    logger.debug("Waiting for download futures to finish.")
    wait_for_futures(futures, executor, database_queue, import_queue, worker_timed_out_flag, cancel_token, logger, worker_timeout)

    if cancel_token.cancelled:
        logger.debug("Shutting down thread pool, waiting for running downloads to complete.")
        executor.shutdown(cancel_futures=True)
        parent_logger.info("Download worker cancelled.")
    else:
        executor.shutdown()
        parent_logger.info("Download worker finished.")

    database_queue.put((None, None))
    import_queue.put(None)

    status_flag.value = False


def wait_for_futures(
    futures: Iterable[Future],
    executor: ThreadPoolExecutor,
    database_queue: queue.Queue,
    import_queue: queue.Queue,
    worker_timed_out_flag: StatusFlag,
    cancel_token: CancellationToken,
    logger: logging.Logger,
    timeout: float = 60.0,
):
    for future_no, future in enumerate(futures, 1):
        if cancel_token.cancelled:
            if not future.done():
                future.cancel()
            continue

        queue_item = wait_for_download(future, future_no, executor, worker_timed_out_flag, logger, timeout)
        if queue_item:
            database_queue.put(
                (
                    queue_item,
                    CollectionFileChange(downloaded_at=utils.get_now(), download_error=queue_item.error_while_downloading),
                )
            )
            import_queue.put(queue_item)


def wait_for_download(
    future: Future, future_no: int, executor: ThreadPoolExecutor, worker_timed_out_flag: StatusFlag, logger: logging.Logger, timeout: float = 60
) -> Optional[CollectionFileQueueItem]:
    try:
        return future.result(timeout)
    except (CancelledError, OperationCancelledError):
        pass
    except TimeoutError:
        worker_timed_out_flag.value = True
        logger.warn("Future no. %i timed out.", future_no)
        executor.shutdown(False, cancel_futures=True)
    except Exception as exc:
        logger.warn("Future no. %i raised an error: %s", future_no, exc)

    return None


def setup_futures(
    executor: ThreadPoolExecutor,
    queue_items: Iterable[CollectionFileQueueItem],
    gdrive_client: GoogleDriveClient,
    debug_mode: bool,
    logger: logging.Logger,
    cancel_token: CancellationToken = None,
) -> list[Future]:
    futures = []
    for queue_item in queue_items:
        if cancel_token and cancel_token.log_if_cancelled(logger, "Requested cancellation during thread pool setup."):
            break

        futures.append(executor.submit(download_gdrive_file, queue_item, gdrive_client, logger, debug_mode, max_download_attempts=3))

    return futures


def download_gdrive_file(
    queue_item: CollectionFileQueueItem,
    gdrive_client: GoogleDriveClient,
    parent_logger: logging.Logger,
    log_stack_trace_on_download_error: bool,
    max_download_attempts: int = 5,
) -> Optional[CollectionFileQueueItem]:
    logger = parent_logger.getChild("downloadGdriveFile")

    if importer_utils.check_if_exists(
        queue_item.item_no,
        queue_item.gdrive_file_size,
        queue_item.target_file_path,
        logger=logger,
    ):
        queue_item.download_file_path = queue_item.target_file_path
        logger.debug("File no. %i already exists: %s", queue_item.item_no, queue_item.download_file_path)

        return queue_item
    else:
        logger.debug("Making sure that file no. %i does not exist.", queue_item.item_no)
        queue_item.target_file_path.unlink(missing_ok=True)  # File also counts as not existing, if the file size differs from the file on gdrive

    try:
        file_contents = download_gdrive_file_contents(
            queue_item.gdrive_file,
            gdrive_client,
            queue_item.cancel_token,
            queue_item.item_no,
            max_download_attempts,
            log_stack_trace_on_download_error,
            logger,
        )
    except (pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError) as download_error:
        queue_item.download_file_path = None
        queue_item.error_while_downloading = True
        raise DownloadFailedError(queue_item.gdrive_file_name, str(download_error), inner_exception=download_error) from download_error

    try:
        queue_item.download_file_path, queue_item.error_while_downloading = write_gdrive_file_to_disk(
            file_contents,
            queue_item.target_file_path,
            queue_item.cancel_token,
            queue_item.item_no,
            queue_item.gdrive_file_name,
            queue_item.gdrive_file_size,
            max_download_attempts,
            logger,
        )
    except IOError as download_error:
        queue_item.download_file_path = None
        queue_item.error_while_downloading = True
        raise DownloadFailedError(queue_item.gdrive_file_name, str(download_error), inner_exception=download_error) from download_error

    return queue_item


def download_gdrive_file_contents(
    gdrive_file: GoogleDriveFile,
    gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    item_no: int,
    max_download_attempts: int,
    log_stack_trace: bool,
    logger: logging.Logger,
):
    download_error: Union[pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError] = None
    gdrive_file_name = utils.get_gdrive_file_name(gdrive_file)

    for attempt in range(max_download_attempts):
        cancel_token.raise_if_cancelled(logger, "Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

        log.gdrive_file_download(logger, attempt, item_no, gdrive_file_name)

        try:
            file_contents = gdrive_client.get_file_contents(gdrive_file)
        except (pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError) as exc:
            download_error = exc
            sleep_for = timedelta(seconds=2 ^ attempt, microseconds=random.randint(0, 1000000))
            log.download_error(logger, item_no, gdrive_file_name, log_stack_trace, download_error, sleep_for)
            time.sleep(sleep_for.total_seconds())  # Wait for a increasing time before retrying as recommended in the google API docs
            continue

        cancel_token.raise_if_cancelled(logger, "Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)
        logger.debug("File no. %i downloaded: %s", item_no, gdrive_file_name)

        return file_contents

    raise download_error


def write_gdrive_file_to_disk(
    file_contents: str,
    file_path: Union[Path, str],
    cancel_token: CancellationToken,
    item_no: int,
    gdrive_file_name: str,
    gdrive_file_size: int,
    max_write_attempts: int,
    logger: logging.Logger,
) -> tuple[Path, bool]:
    if file_contents:
        download_error: IOError = None

        for _ in range(max_write_attempts):
            cancel_token.raise_if_cancelled(logger, "Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

            try:
                with open(file_path, "w") as fp:
                    fp.write(file_contents)

                logger.debug("File no. %i written to disk: %s", item_no, file_path)
            except IOError as exc:
                download_error = exc
                continue

            cancel_token.raise_if_cancelled(logger, "Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

            while not importer_utils.check_if_exists(
                item_no,
                gdrive_file_size,
                file_path,
            ):  # It may take some time for the file contents to be flushed to disk
                cancel_token.raise_if_cancelled(logger, "Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

                time.sleep(0.1)

            return file_path, False

        raise download_error

    return None, True
