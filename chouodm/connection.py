import os
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from bson.raw_bson import RawBSONDocument

from .singleton import Singleton

DEFAULT_ENV_NAME: str = "default"


_connections: dict = {}


class Connection(object, metaclass=Singleton):
    __slots__ = (
        "address",
        "database_name",
        "max_pool_size",
        "server_selection_timeout_ms",
        "connect_timeout_ms",
        "socket_timeout_ms",
        "ssl_cert_path",
        "env_name",
    )

    _connections: dict = {}

    def __init__(
        self,
        address: str,
        database_name: str,
        max_pool_size: int = 250,
        ssl_cert_path: Optional[str] = None,
        server_selection_timeout_ms: int = 60000,
        connect_timeout_ms: int = 30000,
        socket_timeout_ms: int = 60000,
        env_name: str = DEFAULT_ENV_NAME,
    ):
        self.address = address
        self.database_name = database_name
        self.max_pool_size = max_pool_size
        self.ssl_cert_path = ssl_cert_path
        self.server_selection_timeout_ms = server_selection_timeout_ms
        self.connect_timeout_ms = connect_timeout_ms
        self.socket_timeout_ms = socket_timeout_ms
        if env_name not in _connections:
            _connections[env_name] = self

    def _init_mongo_connection(self, connect: bool = False) -> AsyncIOMotorClient:  # type: ignore
        connection_params: dict = {
            "host": self.address,
            "connect": connect,
            "serverSelectionTimeoutMS": self.server_selection_timeout_ms,
            "maxPoolSize": self.max_pool_size,
            "connectTimeoutMS": self.connect_timeout_ms,
            "socketTimeoutMS": self.socket_timeout_ms,
        }
        if self.ssl_cert_path:
            connection_params["tlsCAFile"] = self.ssl_cert_path
            connection_params["tlsAllowInvalidCertificates"] = bool(self.ssl_cert_path)
        client = AsyncIOMotorClient(**connection_params, document_class=RawBSONDocument)
        return client

    def _get_motor_client(self) -> AsyncIOMotorClient:  # type: ignore
        pid = os.getpid()
        if pid in self._connections:
            return self._connections[pid]
        else:
            mongo_connection = self._init_mongo_connection()
            self._connections[os.getpid()] = mongo_connection
            return mongo_connection


def connect(
    address: str,
    database_name: str,
    max_pool_size: int = 100,
    ssl_cert_path: Optional[str] = None,
    server_selection_timeout_ms: int = 60000,
    connect_timeout_ms: int = 30000,
    socket_timeout_ms: int = 60000,
    env_name: Optional[str] = DEFAULT_ENV_NAME,
) -> Connection:
    """init connection to mongodb

    Args:
        address (str): full connection string
        database_name (str): mongo db name
        max_pool_size (int, optional): max connection pool. Defaults to 100.
        ssl_cert_path (Optional[str], optional): path to ssl cert. Defaults to None.
        server_selection_timeout_ms (int, optional): ServerSelectionTimeoutMS. Defaults to 60000.
        connect_timeout_ms (int, optional): ConnectionTimeoutMS. Defaults to 30000.
        socket_timeout_ms (int, optional): SocketTimeoutMS. Defaults to 60000.
        env_name (Optional[str], optional): connection env name. Defaults to None.

    Returns:
        Connection: CHOUODM connection
    """
    os.environ["CHOUODM_DATABASE"] = database_name
    os.environ["CHOUODM_ADDRESS"] = address
    os.environ["CHOUODM_ENV_NAME"] = env_name or DEFAULT_ENV_NAME
    os.environ["CHOUODM_MAX_POOL_SIZE"] = str(max_pool_size)
    os.environ["CHOUODM_SERVER_SELECTION_TIMOUT_MS"] = str(server_selection_timeout_ms)
    os.environ["CHOUODM_CONNECT_TIMEOUT_MS"] = str(connect_timeout_ms)
    os.environ["CHOUODM_SOCKET_TIMEOUT_MS"] = str(socket_timeout_ms)
    if ssl_cert_path:
        os.environ["CHOUODM_SSL_CERT_PATH"] = ssl_cert_path
    connection = Connection(
        address=address,
        database_name=database_name,
        env_name=env_name or DEFAULT_ENV_NAME,
        max_pool_size=max_pool_size,
        server_selection_timeout_ms=server_selection_timeout_ms,
        connect_timeout_ms=connect_timeout_ms,
        socket_timeout_ms=socket_timeout_ms,
        ssl_cert_path=ssl_cert_path,
    )
    return connection
