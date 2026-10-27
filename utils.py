import logging
import os
import sys

import requests


def logging_config():
    level = logging.DEBUG if os.getenv('LEVEL_LOGGING') == '0' else logging.INFO
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        level=level,
        stream=sys.stdout,
    )


def is_retryable(exc):
    """Erros transitórios: vale a pena tentar de novo."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        return status == 429 or status >= 500
    return getattr(exc, 'retryable', False)
