"""Real TLS tests for the bounded target-owner channel.

Author: SqlRush <sqlrush@gmail.com>
Certificates are ephemeral test-only identities, not deployment credentials.
"""

import hashlib
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournal, TargetJournalError
from test_target_journal import identity
try:
    from target_transport import serve_one
except ModuleNotFoundError:
    serve_one = None


class TargetTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="pgrac-target-tls-")
        cls.directory = Path(cls.temporary.name)
        cls.addClassCleanup(cls.temporary.cleanup)
        for name in ("server", "client", "other"):
            result = subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                 "-subj", f"/CN={name}", "-keyout", str(cls.directory / (name + ".key")),
                 "-out", str(cls.directory / (name + ".pem"))],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            if result.returncode != 0:
                raise AssertionError("test certificate generation failed")
        cls.pin = hashlib.sha256(ssl.PEM_cert_to_DER_cert(
            (cls.directory / "client.pem").read_text())).hexdigest()

    def context(self, *, server, certificate=True, trust="client"):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.options |= ssl.OP_NO_TICKET
        if server:
            context.num_tickets = 0
            context.load_verify_locations(str(self.directory / (trust + ".pem")))
            name = "server"
        else:
            context.load_verify_locations(str(self.directory / "server.pem"))
            name = "client"
        if certificate:
            context.load_cert_chain(str(self.directory / (name + ".pem")),
                                    str(self.directory / (name + ".key")))
        return context

    def exchange(self, wire, *, pins=None, server=None, client=None, handler=None,
                 seconds=2, receive=True, after_send=None):
        self.assertTrue(callable(serve_one), "authenticated target transport is missing")
        calls, errors, response = [], [], b""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(3)
        deadline = time.monotonic_ns() + int(seconds * 1_000_000_000)

        def dispatch(request):
            calls.append(request)
            return handler(request) if handler else b"reply-not-a-proof"

        def target():
            try:
                connection, _ = listener.accept()
                serve_one(connection, server or self.context(server=True),
                          (self.pin,) if pins is None else pins, deadline, dispatch)
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=target, daemon=True)
        worker.start()
        try:
            with socket.create_connection(listener.getsockname(), timeout=3) as plain:
                try:
                    with (client or self.context(server=False)).wrap_socket(
                            plain, server_hostname="server") as connection:
                        connection.settimeout(3)
                        connection.sendall(wire)
                        if after_send:
                            after_send()
                        if receive:
                            while True:
                                chunk = connection.recv(8192)
                                if not chunk:
                                    break
                                response += chunk
                except (ssl.SSLError, OSError):
                    pass
        finally:
            worker.join(4)
            listener.close()
        self.assertFalse(worker.is_alive(), "target channel exceeded its envelope")
        return calls, errors, response, deadline

    def refused(self, result):
        calls, errors, response, _ = result
        self.assertEqual(calls, [])
        self.assertEqual(response, b"")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TargetJournalError)
        self.assertRegex(str(errors[0]), r"^TARGET_TRANSPORT_[A-Z_]+$")

    def test_real_authenticated_exchange_preserves_exact_payload_peer_and_deadline(self):
        body = b"request-no-authority"
        calls, errors, response, deadline = self.exchange(struct.pack("!I", len(body)) + body)
        self.assertEqual(errors, [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].payload, body)
        self.assertEqual(calls[0].peer_sha256, self.pin)
        self.assertEqual(calls[0].deadline_mono_ns, deadline)
        self.assertEqual(response, struct.pack("!I", 17) + b"reply-not-a-proof")

    def test_valid_but_unlisted_peer_is_not_authorized(self):
        self.refused(self.exchange(b"\0\0\0\1x", pins=("ab" * 32,)))

    def test_missing_certificate_or_wrong_trust_never_dispatches(self):
        self.refused(self.exchange(b"\0\0\0\1x", client=self.context(server=False, certificate=False)))
        self.refused(self.exchange(b"\0\0\0\1x", server=self.context(server=True, trust="other")))

    def test_empty_or_oversized_frame_never_dispatches(self):
        for length in (0, 4097, 0xFFFFFFFF):
            with self.subTest(length=length):
                self.refused(self.exchange(struct.pack("!I", length)))

    def test_partial_eof_and_partial_timeout_never_dispatch(self):
        self.refused(self.exchange(b"\0\0\0\4x", receive=False))
        start = time.monotonic()
        self.refused(self.exchange(b"\0\0\0\4x", seconds=0.5))
        self.assertLess(time.monotonic() - start, 3)

    def test_unsafe_context_and_empty_allowlist_refused(self):
        context = self.context(server=True)
        context.verify_mode = ssl.CERT_NONE
        self.refused(self.exchange(b"\0\0\0\1x", server=context))
        self.refused(self.exchange(b"\0\0\0\1x", pins=()))

    def test_handler_failure_does_not_echo_secrets_or_report_success(self):
        def fail(_request):
            raise ValueError("secret request credentials")
        calls, errors, response, _ = self.exchange(b"\0\0\0\1x", handler=fail)
        self.assertEqual(len(calls), 1)
        self.assertEqual(response, b"")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TargetJournalError)
        self.assertNotIn("secret", str(errors[0]))

    def test_typed_handler_failure_is_not_an_unsanitized_transport_error(self):
        def fail(_request):
            raise TargetJournalError("secret-handler-material")
        calls, errors, response, _ = self.exchange(b"\0\0\0\1x", handler=fail)
        self.assertEqual(len(calls), 1)
        self.assertEqual(response, b"")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TargetJournalError)
        self.assertNotIn("secret", str(errors[0]))

    def test_maximum_request_and_reply_are_complete(self):
        body = b"x" * 4096
        calls, errors, response, _ = self.exchange(struct.pack("!I", len(body)) + body,
                                                  handler=lambda request: request.payload)
        self.assertEqual(errors, [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(response, struct.pack("!I", len(body)) + body)

    def test_late_handler_has_no_reply_and_no_new_deadline(self):
        def expire(request):
            delay = max(0, (request.deadline_mono_ns - time.monotonic_ns()) / 1_000_000_000)
            threading.Event().wait(delay + 0.01)
            return b"expired"
        calls, errors, response, _ = self.exchange(b"\0\0\0\1x", handler=expire, seconds=0.5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(response, b"")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TargetJournalError)

    def test_disconnect_does_not_revoke_already_owned_durable_work(self):
        arrived, finish = threading.Event(), threading.Event()
        with tempfile.TemporaryDirectory(prefix="pgrac-tls-owner-") as directory:
            directory = str(Path(directory).resolve())
            with TargetJournal(directory, "ab" * 32, initialize=True, owner_uid=os.geteuid()) as journal:
                def own(_request):
                    journal.deny(identity())
                    arrived.set()
                    if not finish.wait(2):
                        raise AssertionError("client did not detach")
                    return b"not-a-proof"
                def detach():
                    self.assertTrue(arrived.wait(2))
                    finish.set()
                calls, errors, response, _ = self.exchange(b"\0\0\0\1x", handler=own,
                                                          receive=False, after_send=detach)
                self.assertEqual(len(calls), 1)
                self.assertEqual(response, b"")
                self.assertTrue(all(isinstance(error, TargetJournalError) for error in errors))
            with TargetJournal(directory, "ab" * 32, owner_uid=os.geteuid()) as journal:
                self.assertEqual([state.identity for state in journal.denied()], [identity()])

    def test_all_context_guards_are_effective(self):
        for change in (lambda c: setattr(c, "minimum_version", ssl.TLSVersion.TLSv1_2),
                       lambda c: setattr(c, "num_tickets", 1),
                       lambda c: setattr(c, "options", c.options & ~ssl.OP_NO_TICKET),
                       lambda c: setattr(c, "keylog_filename", str(self.directory / "test-keylog"))):
            context = self.context(server=True)
            change(context)
            self.refused(self.exchange(b"\0\0\0\1x", server=context))
        for pins in ((self.pin, self.pin), ("0" * 64,), [self.pin], (True,)):
            self.refused(self.exchange(b"\0\0\0\1x", pins=pins))

    def test_expired_connection_never_dispatches(self):
        self.refused(self.exchange(b"\0\0\0\1x", seconds=-1))

    def test_reply_is_bounded_and_not_coerced(self):
        for reply in (b"", b"x" * 4097, "not-bytes"):
            with self.subTest(kind=type(reply), size=len(reply)):
                calls, errors, response, _ = self.exchange(b"\0\0\0\1x", handler=lambda _r: reply)
                self.assertEqual(len(calls), 1)
                self.assertEqual(response, b"")
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], TargetJournalError)


if __name__ == "__main__":
    unittest.main()
