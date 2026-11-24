"""Minimal blocking libpq binding (ctypes) for the long-run workload client.

Loads the libpq of the installation under test, so no extra package is
needed. One Connection belongs to one thread; cancel() may be called from
another thread. Errors are returned as data with SQLSTATE and server fields.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import ctypes
from pathlib import Path
from typing import List, NamedTuple, Optional

CONNECTION_OK = 0
PGRES_COMMAND_OK, PGRES_TUPLES_OK = 1, 2
_FIELDS = dict(severity=b'V', sqlstate=b'C', message=b'M', detail=b'D', hint=b'H',
               source_file=b'F', source_line=b'L', source_function=b'R')
_LIBRARY_NAMES = ('libpq.so.5', 'libpq.so', 'libpq.5.dylib', 'libpq.dylib')


class Result(NamedTuple):
    """Outcome of one statement. ok is False for every server or link error."""
    ok: bool
    command: str            # command tag, e.g. 'UPDATE 1', 'COMMIT', 'ROLLBACK'
    rows: List[List[Optional[str]]]
    severity: Optional[str]
    sqlstate: Optional[str]
    message: Optional[str]
    detail: Optional[str]
    hint: Optional[str]
    location: Optional[str]
    connection_lost: bool


def find_library(install: Path) -> Path:
    for name in _LIBRARY_NAMES:
        path = Path(install) / 'lib' / name
        if path.exists():
            return path
    raise FileNotFoundError('libpq not found under ' + str(Path(install) / 'lib'))


def load(path: Path) -> ctypes.CDLL:
    """CDLL releases the interpreter lock while a libpq call blocks."""
    lib = ctypes.CDLL(str(path))
    pointer, text = ctypes.c_void_p, ctypes.c_char_p
    for name, restype, argtypes in (
            ('PQconnectdb', pointer, [text]), ('PQstatus', ctypes.c_int, [pointer]),
            ('PQerrorMessage', text, [pointer]), ('PQfinish', None, [pointer]),
            ('PQexec', pointer, [pointer, text]), ('PQresultStatus', ctypes.c_int, [pointer]),
            ('PQresultErrorField', text, [pointer, ctypes.c_int]),
            ('PQcmdStatus', text, [pointer]), ('PQclear', None, [pointer]),
            ('PQntuples', ctypes.c_int, [pointer]), ('PQnfields', ctypes.c_int, [pointer]),
            ('PQgetvalue', text, [pointer, ctypes.c_int, ctypes.c_int]),
            ('PQgetisnull', ctypes.c_int, [pointer, ctypes.c_int, ctypes.c_int]),
            ('PQbackendPID', ctypes.c_int, [pointer]), ('PQgetCancel', pointer, [pointer]),
            ('PQcancel', ctypes.c_int, [pointer, ctypes.c_char_p, ctypes.c_int]),
            ('PQfreeCancel', None, [pointer])):
        function = getattr(lib, name)
        function.restype, function.argtypes = restype, argtypes
    return lib


def _text(value: Optional[bytes]) -> Optional[str]:
    return None if value is None else value.decode('utf-8', errors='replace').rstrip('\n')


def quote_conninfo(value: object) -> str:
    return "'" + str(value).replace('\\', '\\\\').replace("'", "\\'") + "'"


class Connection:
    def __init__(self, lib: ctypes.CDLL, conninfo: str) -> None:
        self.lib, self.conninfo = lib, conninfo
        self.handle: Optional[int] = None
        self.cancel_handle: Optional[int] = None
        self.backend_pid: Optional[int] = None

    def connect(self) -> Optional[str]:
        """Return None on success, otherwise the libpq failure text."""
        self.close()
        handle = self.lib.PQconnectdb(self.conninfo.encode())
        if not handle:
            return 'libpq could not allocate a connection'
        if self.lib.PQstatus(handle) != CONNECTION_OK:
            message = _text(self.lib.PQerrorMessage(handle)) or 'connection failed'
            self.lib.PQfinish(handle)
            return message
        self.handle = handle
        self.backend_pid = self.lib.PQbackendPID(handle)
        self.cancel_handle = self.lib.PQgetCancel(handle)
        return None

    def connected(self) -> bool:
        return self.handle is not None and self.lib.PQstatus(self.handle) == CONNECTION_OK

    def execute(self, sql: str) -> Result:
        if self.handle is None:
            return Result(False, '', [], None, None, 'not connected', None, None, None, True)
        result = self.lib.PQexec(self.handle, sql.encode())
        lost = self.lib.PQstatus(self.handle) != CONNECTION_OK
        if not result:
            message = _text(self.lib.PQerrorMessage(self.handle)) or 'no result'
            return Result(False, '', [], None, None, message, None, None, None, lost)
        try:
            status = self.lib.PQresultStatus(result)
            command = _text(self.lib.PQcmdStatus(result)) or ''
            if status in (PGRES_COMMAND_OK, PGRES_TUPLES_OK):
                rows = [[None if self.lib.PQgetisnull(result, r, c)
                         else _text(self.lib.PQgetvalue(result, r, c))
                         for c in range(self.lib.PQnfields(result))]
                        for r in range(self.lib.PQntuples(result))]
                return Result(True, command, rows, None, None, None, None, None, None, lost)
            field = {name: _text(self.lib.PQresultErrorField(result, ord(code)))
                     for name, code in _FIELDS.items()}
            location = None
            if field['source_file']:
                location = '%s, %s:%s' % (field['source_function'], field['source_file'],
                                          field['source_line'])
            message = field['message'] or _text(self.lib.PQerrorMessage(self.handle)) \
                or 'statement failed without a message'
            return Result(False, command, [], field['severity'], field['sqlstate'], message,
                          field['detail'], field['hint'], location, lost)
        finally:
            self.lib.PQclear(result)

    def cancel(self) -> bool:
        """Ask the server to cancel the running statement; thread-safe."""
        if self.cancel_handle is None:
            return False
        buffer = ctypes.create_string_buffer(256)
        return bool(self.lib.PQcancel(self.cancel_handle, buffer, len(buffer)))

    def close(self) -> None:
        if self.cancel_handle is not None:
            self.lib.PQfreeCancel(self.cancel_handle)
            self.cancel_handle = None
        if self.handle is not None:
            self.lib.PQfinish(self.handle)
            self.handle = None
