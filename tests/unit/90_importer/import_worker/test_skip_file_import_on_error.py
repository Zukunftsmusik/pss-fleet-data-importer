import pytest
from pss_fleet_data import CollectionMetadata

from fake_classes import FakeFileSystem
from src.app.importer.import_worker import skip_file_import_on_error
from src.app.models.queue_item import QueueItem


def test_return_true_if_queue_item_cancelled(queue_item: QueueItem):
    queue_item.cancel_token.cancel()

    assert skip_file_import_on_error(queue_item) is True


def test_return_true_if_queue_item_error_while_download(queue_item: QueueItem):
    queue_item.error_while_downloading = True

    assert skip_file_import_on_error(queue_item) is True


test_cases_empty_json = [
    # obj
    pytest.param(None, id="none"),
    pytest.param({}, id="dict"),
    pytest.param([], id="list"),
    pytest.param("", id="str"),
]


@pytest.mark.parametrize(["obj"], test_cases_empty_json)
def test_return_true_if_file_contains_empty_json(obj: object, queue_item: QueueItem, filesystem: FakeFileSystem):
    filesystem.dump_json(queue_item.target_file_path, obj)

    assert skip_file_import_on_error(queue_item, filesystem=filesystem) is True


def test_return_false_else(queue_item: QueueItem, filesystem: FakeFileSystem, collection_metadata_out: CollectionMetadata):
    filesystem.dump_json(queue_item.target_file_path, collection_metadata_out.model_dump_json())

    assert skip_file_import_on_error(queue_item, filesystem=filesystem) is False
