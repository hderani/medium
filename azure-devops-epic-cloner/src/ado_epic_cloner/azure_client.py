from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication


def get_connection(url: str, pat: str) -> Connection:
    credentials = BasicAuthentication("", pat)
    return Connection(base_url=url, creds=credentials)