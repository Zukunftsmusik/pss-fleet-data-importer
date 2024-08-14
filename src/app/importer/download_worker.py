import logging
import queue
import random
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, Union

import pydrive2.files

from ..core import utils
from ..core.gdrive import GDriveFile, GoogleDriveClient
from ..core.models.cancellation_token import CancellationToken, OperationCancelledError
from ..core.models.collection_file_change import CollectionFileChange
from ..core.models.filesystem import FileSystem
from ..core.models.status import StatusFlag
from ..log.log_importer import download_worker as log
from ..models.queue_item import QueueItem
from . import utils as importer_utils
from .exceptions import DownloadFailedError


class DownloadFunction(Protocol):
    def __call__(self, queue_item: QueueItem, *args, cancel_token: CancellationToken, **kwargs) -> QueueItem:
        pass


def worker(
    queue_items: Iterable[QueueItem],
    gdrive_client: GoogleDriveClient,
    thread_pool_size: int,
    database_queue: queue.Queue,
    import_queue: queue.Queue,
    status_flag: StatusFlag,
    worker_timeout: float,
    worker_timed_out_flag: StatusFlag,
    cancel_token: CancellationToken,
    debug_mode: bool,
    filesystem: FileSystem = FileSystem(),
):
    status_flag.value = True
    worker_timed_out_flag.value = False

    log.download_worker_started()

    log.thread_pool_setup(thread_pool_size)
    executor = ThreadPoolExecutor(thread_pool_size, thread_name_prefix="Download gdrive file")
    futures = setup_futures(
        executor,
        queue_items,
        download_gdrive_file,
        cancel_token=cancel_token,
        additional_func_args=(gdrive_client, debug_mode),
        max_download_attempts=3,
        filesystem=filesystem,
    )

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
    futures: Iterable[tuple[Future, QueueItem]],
    executor: ThreadPoolExecutor,
    database_queue: queue.Queue,
    import_queue: queue.Queue,
    worker_timed_out_flag: StatusFlag,
    cancel_token: CancellationToken,
    timeout: float = 60.0,
):
    for future, queue_item in futures:
        if cancel_token.cancelled:
            if not future.done():
                future.cancel()
            continue

        wait_for_download(future, queue_item, queue_item.item_no, executor, worker_timed_out_flag, timeout)
        if queue_item.downloaded:
            database_queue.put(
                (
                    queue_item,
                    CollectionFileChange(downloaded_at=utils.get_now(), download_error=queue_item.error_while_downloading),
                )
            )
            import_queue.put(queue_item)


def wait_for_download(
    future: Future,
    queue_item: QueueItem,
    executor: ThreadPoolExecutor,
    worker_timed_out_flag: StatusFlag,
    timeout: float = 60,
) -> QueueItem:
    try:
        future.result(timeout=timeout)
    except (CancelledError, OperationCancelledError):
        queue_item.downloaded = False
        queue_item.error_while_downloading = False
    except TimeoutError:
        queue_item.downloaded = False
        queue_item.error_while_downloading = True
        worker_timed_out_flag.value = True

        executor.shutdown(False, cancel_futures=True)

        log.future_timeout(queue_item.item_no)
    except Exception as exc:
        queue_item.downloaded = False
        queue_item.error_while_downloading = True

        log.future_error(queue_item.item_no, exc)
    else:
        queue_item.downloaded = True
        queue_item.error_while_downloading = False

    return queue_item


def setup_futures(
    executor: ThreadPoolExecutor,
    queue_items: Iterable[QueueItem],
    func: DownloadFunction,
    cancel_token: Optional[CancellationToken] = None,
    additional_func_args: Optional[Iterable[Any]] = None,
    **func_kwargs,
) -> list[tuple[Future, QueueItem]]:
    futures = []
    additional_func_args = additional_func_args or ()
    for queue_item in queue_items:
        if cancel_token and cancel_token.log_if_cancelled("Requested cancellation during thread pool setup."):
            break

        futures.append(
            (
                executor.submit(func, queue_item, *additional_func_args, cancel_token=cancel_token, **func_kwargs),
                queue_item,
            )
        )

    return futures


def download_gdrive_file(
    queue_item: QueueItem,
    gdrive_client: GoogleDriveClient,
    log_stack_trace_on_download_error: bool,
    max_download_attempts: int = 3,
    filesystem: FileSystem = FileSystem(),
):
    if file_already_downloaded(queue_item, filesystem=filesystem):
        log.file_exists(queue_item.item_no, queue_item.target_file_path)
        return

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
        raise DownloadFailedError(queue_item.gdrive_file.name, str(download_error), inner_exception=download_error) from download_error

    if not file_contents:
        raise DownloadFailedError(queue_item.gdrive_file.name, "The downloaded file was empty.")

    try:
        write_gdrive_file_to_disk(
            file_contents,
            queue_item.target_file_path,
            queue_item.cancel_token,
            queue_item.item_no,
            queue_item.gdrive_file.name,
            max_download_attempts,
        )
    except IOError as io_error:
        raise DownloadFailedError(queue_item.gdrive_file.name, str(io_error), inner_exception=io_error) from io_error

    log.downloaded_file(queue_item.item_no, queue_item.target_file_path)


def file_already_downloaded(queue_item: QueueItem, filesystem: FileSystem = FileSystem()) -> bool:
    if importer_utils.check_if_exists(queue_item.target_file_path, queue_item.gdrive_file.size, filesystem):
        log.file_exists(queue_item.item_no, queue_item.target_file_path)
        return True

    # File also counts as not existing, if the file size differs from the file on gdrive
    log.file_delete(queue_item.item_no)
    filesystem.delete(queue_item.target_file_path, missing_ok=True)
    return False


def download_gdrive_file_contents(
    gdrive_file: GDriveFile,
    gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    item_no: int,
    max_download_attempts: int,
    log_stack_trace: bool,
) -> str:
    download_error: Union[pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError] = None

    for attempt in range(max_download_attempts):
        cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file.name, log_level=logging.DEBUG)

        log.downloading_gdrive_file(attempt, item_no, gdrive_file.name)

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
    max_write_attempts: int,
    filesystem: FileSystem = FileSystem(),
):
    io_error: IOError = None

    for _ in range(max_write_attempts):
        cancel_token.raise_if_cancelled("Cancelled download of file no. %i: %s", item_no, gdrive_file_name, log_level=logging.DEBUG)

        try:
            filesystem.write(file_path, file_contents)
            return
        except IOError as exc:
            io_error = exc
            continue

    raise io_error
