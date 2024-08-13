import logging
import queue
import random
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Optional, Union

import pydrive2.files

from ..core import utils
from ..core.gdrive import GDriveFile, GoogleDriveClient
from ..core.models.cancellation_token import CancellationToken, OperationCancelledError
from ..core.models.collection_file_change import CollectionFileChange
from ..core.models.filesystem import FileSystem
from ..core.models.status import StatusFlag
from ..log.log_importer import download_worker as log
from ..models.queue_item import CollectionFileQueueItem
from . import utils as importer_utils
from .exceptions import DownloadFailedError


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
):
    status_flag.value = True
    worker_timed_out_flag.value = False

    log.download_worker_started()

    log.thread_pool_setup(thread_pool_size)
    executor = ThreadPoolExecutor(thread_pool_size, thread_name_prefix="Download gdrive file")
    futures: list[Future] = setup_futures(executor, queue_items, gdrive_client, debug_mode, cancel_token=cancel_token)

    log.wait_for_futures()
    wait_for_futures(futures, executor, database_queue, import_queue, worker_timed_out_flag, cancel_token, worker_timeout)

    if cancel_token.cancelled:
        log.thread_pool_cancel()
        executor.shutdown(cancel_futures=True)
    else:
        executor.shutdown()

    log.download_worker_ended(cancel_token)

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
    timeout: float = 60.0,
):
    for future_no, future in enumerate(futures, 1):
        if cancel_token.cancelled:
            if not future.done():
                future.cancel()
            continue

        queue_item = wait_for_download(future, future_no, executor, worker_timed_out_flag, timeout)
        if queue_item:
            database_queue.put(
                (
                    queue_item,
                    CollectionFileChange(downloaded_at=utils.get_now(), download_error=queue_item.error_while_downloading),
                )
            )
            import_queue.put(queue_item)


def wait_for_download(
    future: Future,
    future_no: int,
    executor: ThreadPoolExecutor,
    worker_timed_out_flag: StatusFlag,
    timeout: float = 60,
) -> Optional[CollectionFileQueueItem]:
    try:
        return future.result(timeout)
    except (CancelledError, OperationCancelledError):
        pass
    except TimeoutError:
        worker_timed_out_flag.value = True
        executor.shutdown(False, cancel_futures=True)

        log.future_timeout(future_no)
    except Exception as exc:
        log.future_error(future_no, exc)

    return None


def setup_futures(
    executor: ThreadPoolExecutor,
    queue_items: Iterable[CollectionFileQueueItem],
    gdrive_client: GoogleDriveClient,
    debug_mode: bool,
    cancel_token: CancellationToken = None,
) -> list[Future]:
    futures = []
    for queue_item in queue_items:
        if cancel_token and cancel_token.log_if_cancelled("Requested cancellation during thread pool setup."):
            break

        futures.append(executor.submit(download_gdrive_file, queue_item, gdrive_client, debug_mode, max_download_attempts=3))

    return futures


def download_gdrive_file(
    queue_item: CollectionFileQueueItem,
    gdrive_client: GoogleDriveClient,
    log_stack_trace_on_download_error: bool,
    max_download_attempts: int = 3,
    filesystem: FileSystem = FileSystem(),
) -> Optional[CollectionFileQueueItem]:
    if importer_utils.check_if_exists(queue_item.target_file_path, queue_item.gdrive_file.size, filesystem):
        queue_item.download_file_path = queue_item.target_file_path
        log.file_exists(queue_item.item_no, queue_item.download_file_path)
        return queue_item
    else:
        log.file_delete(queue_item.item_no)
        # File also counts as not existing, if the file size differs from the file on gdrive
        filesystem.delete(queue_item.target_file_path, missing_ok=True)

    try:
        file_contents = download_gdrive_file_contents(
            queue_item.gdrive_file,
            gdrive_client,
            queue_item.cancel_token,
            queue_item.item_no,
            max_download_attempts,
            log_stack_trace_on_download_error,
        )
    except (pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError) as download_error:
        queue_item.download_file_path = None
        queue_item.error_while_downloading = True
        raise DownloadFailedError(queue_item.gdrive_file.name, str(download_error), inner_exception=download_error) from download_error

    try:
        queue_item.download_file_path, queue_item.error_while_downloading = write_gdrive_file_to_disk(
            file_contents,
            queue_item.target_file_path,
            queue_item.cancel_token,
            queue_item.item_no,
            queue_item.gdrive_file.name,
            queue_item.gdrive_file.size,
            max_download_attempts,
        )
    except IOError as download_error:
        queue_item.download_file_path = None
        queue_item.error_while_downloading = True
        raise DownloadFailedError(queue_item.gdrive_file.name, str(download_error), inner_exception=download_error) from download_error

    return queue_item


def download_gdrive_file_contents(
    gdrive_file: GDriveFile,
    gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    item_no: int,
    max_download_attempts: int,
    log_stack_trace: bool,
):
    download_error: Union[pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError] = None

    for attempt in range(max_download_attempts):
        cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file.name, log_level=logging.DEBUG)

        log.download_gdrive_file(attempt, item_no, gdrive_file.name)

        try:
            file_contents = gdrive_client.get_file_contents(gdrive_file)
        except (pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError) as exc:
            download_error = exc
            sleep_for = timedelta(seconds=2 ^ attempt, microseconds=random.randint(0, 1000000))
            log.download_error(item_no, gdrive_file.name, log_stack_trace, download_error, sleep_for)
            time.sleep(sleep_for.total_seconds())  # Wait for a increasing time before retrying as recommended in the google API docs
            continue

        cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file.name, log_level=logging.DEBUG)
        log.download_completed(item_no, gdrive_file.name)

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
    filesystem: FileSystem = FileSystem(),
) -> tuple[Path, bool]:
    if file_contents:
        download_error: IOError = None

        for _ in range(max_write_attempts):
            cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

            try:
                filesystem.write(file_path, file_contents)
            except IOError as exc:
                download_error = exc
                continue

            log.write_file_to_disk(item_no, file_path)
            cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

            # It may take some time for the file contents to be flushed to disk
            while not importer_utils.check_if_exists(file_path, gdrive_file_size):
                cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

                time.sleep(0.1)

            return file_path, False

        raise download_error

    return None, True
