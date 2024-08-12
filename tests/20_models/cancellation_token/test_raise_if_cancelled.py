import pytest

from src.app.core.models.cancellation_token import OperationCancelledError
from src.app.models import CancellationToken


def test_cancelled(cancel_token: CancellationToken):
    cancel_token.cancel()

    with pytest.raises(OperationCancelledError):
        cancel_token.raise_if_cancelled()


def test_not_cancelled(cancel_token: CancellationToken):
    cancel_status = cancel_token.raise_if_cancelled()
    assert cancel_status is False
