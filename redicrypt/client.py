from redis import Redis, ConnectionPool
import inspect


class Client(object):

    REDIS_CMDS = {
        "encrypt": "RC.SETENC",
        "decrypt": "RC.GETENC",
        "hash": "RC.SETHASH",
        "b64encode": "RC.SETB64",
        "b64decode": "RC.GETB64",
        "b64encrypt": "RC.BSETENC",
        "b64decrypt": "RC.BGETENC",
    }

    SUPPORTED_HASHES = [
        "sha1",
        "sha224",
        "sha256",
        "sha3-224",
        "sha3-256",
        "sha3-384",
        "sha3-512",
        "whirlpool",
    ]

    def __init__(self, index_name: str, host='localhost', port=6379, conn=None, password=None):
        """Creates a new redis client, attaching to the specified index.

        Either a host/port combination must be passed in, or a connection object.
        """

        self.index_name = index_name

        if conn is not None:
            self.REDIS = conn
        else:
            self.REDIS = Redis(
                connection_pool=ConnectionPool(host=host, port=port, password=password,
                decode_responses=True)
            )

    def _run(self, **kwargs):
        caller = inspect.stack()[1].function
        _cmd = self.REDIS_CMDS.get(caller, None)
        if _cmd is None:
            raise AttributeError("%s has no associated redis command." % caller)

        redis_cmd = f"{_cmd} {' '.join(kwargs.values())}"
        return self.REDIS.execute_command(redis_cmd)

    def hash(self, hashtype: str, key: str, value: str):
        if hashtype not in self.SUPPORTED_HASHES:
            raise AttributeError("%s is not a supported hash type." % hashtype)
        return self._run(hashtype=hashtype, key=key, val=value)

    def encrypt(self, key: str, value: str):
        """Store a value, encrypted in a key."""
        return self._run(key=key, val=value)

    def decrypt(self, key: str):
        """Returns the decrypted value of a key."""
        return self._run(key=key)

    def b64encode(self, key: str, value: str):
        """Given a value, store it base64 encoded"""
        return self._run(key=key, value=value)

    def b64decode(self, key: str):
        """Return the decoded value from a base64 encoded key"""
        return self._run(key=key)

    def b64encrypt(self, key: str, value: str):
        """Store an encrypted value, in a base64 encoded key"""
        return self._run(key=key, value=value)

    def b64decrypt(self, key: str):
        """Return the decrypted value of a base64 encoded key"""
        return self._run(key=key)
