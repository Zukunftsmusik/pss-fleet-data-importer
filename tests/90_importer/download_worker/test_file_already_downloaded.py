from src.app.core.models.filesystem import FileSystem
from src.app.importer.download_worker import file_already_downloaded
from src.app.models.queue_item import QueueItem
from tests.fake_classes import FakeGDriveFile


def test_return_true_if_exists_with_correct_size(filesystem: FileSystem, fake_queue_item: QueueItem, fake_gdrive_file: FakeGDriveFile):
    filesystem.write(fake_queue_item.target_file_path, fake_gdrive_file.content)

    assert file_already_downloaded(fake_queue_item, filesystem=filesystem) is True


def test_return_false_if_not_exists(filesystem: FileSystem, fake_queue_item: QueueItem):
    assert file_already_downloaded(fake_queue_item, filesystem=filesystem) is False


def test_return_false_if_size_not_match(filesystem: FileSystem, fake_queue_item: QueueItem):
    filesystem.write(fake_queue_item.target_file_path, "abc")

    assert file_already_downloaded(fake_queue_item, filesystem=filesystem) is False
