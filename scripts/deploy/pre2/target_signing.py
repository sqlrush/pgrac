"""Pinned target-side signing invocation; not an isolation fact source.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import dataclass
import os
import stat
import subprocess
import sys
import time

from target_journal import TargetJournalError
from target_mapping import VerifierPin, _hex_digest, _pin_verifier, _u64
from target_registry import _open_registry


@dataclass(frozen=True)
class SigningPin:
    executable: VerifierPin
    seed_path: str
    public_key: bytes


def sign_drain_body(pin, body, deadline_mono_ns):
    """Restricted owner only; global obligations must be proven before calling.

    No secret bytes are read by this process. The independently pinned C child
    verifies the key descriptor and wipes its private input after signing.
    This codec call does not authenticate a request or establish native facts.
    """
    executable_fd = seed_fd = None
    try:
        if (sys.platform != "linux" or type(pin) is not SigningPin
                or type(pin.executable) is not VerifierPin
                or not _hex_digest(pin.executable.sha256)
                or type(pin.executable.owner_uid) is not int
                or pin.executable.owner_uid != os.geteuid()
                or type(pin.public_key) is not bytes or len(pin.public_key) != 32
                or type(body) is not bytes or not 176 <= len(body) <= 1200
                or not _u64(deadline_mono_ns) or deadline_mono_ns <= time.monotonic_ns()):
            raise TargetJournalError("TARGET_SIGN_ARGUMENT")
        seed_fd = _open_registry(pin.seed_path, pin.executable.owner_uid)
        info = os.fstat(seed_fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != pin.executable.owner_uid
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size != 32):
            raise TargetJournalError("TARGET_SIGN_KEY_FILE")
        executable_fd = _pin_verifier(pin.executable)
        remaining = (deadline_mono_ns - time.monotonic_ns()) / 1_000_000_000
        if remaining <= 0:
            raise TargetJournalError("TARGET_SIGN_ENVELOPE_EXPIRED")
        result = subprocess.run(
            [f"/proc/self/fd/{executable_fd}", pin.public_key.hex(), str(seed_fd)],
            input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            pass_fds=(executable_fd, seed_fd), env={"LC_ALL": "C"}, timeout=remaining, check=False)
        if (result.returncode != 0 or result.stderr or len(result.stdout) != len(body) + 64
                or result.stdout[:-64] != body or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_SIGN_UNPROVEN")
        return result.stdout
    except TargetJournalError as error:
        raise error from None
    except Exception:
        raise TargetJournalError("TARGET_SIGN_UNPROVEN") from None
    finally:
        for descriptor in (executable_fd, seed_fd):
            if descriptor is not None:
                os.close(descriptor)
