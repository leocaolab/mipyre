"""Private copies of installed packages: a directory only this user can write, names in it held
by a lock, and a copy-on-write clone of a package into it.

The launcher imports this as ``mipyre._clone`` to pin the `mipyre` its spawned processes import. A
sub-interpreter worker with ``runtime="isolated"`` routes runs the same source in its bootstrap
namespace (the engine embeds it, before the isolation adapter), because that runs before the
``mipyre`` package can be imported. So it imports only the standard library, and `shutil`, which
only a fallback copy or a cleanup needs, only there: every such worker runs this file as it starts.
"""

import fcntl
import logging
import os
import stat
import subprocess
import sys


class PrivateCopyError(RuntimeError):
    """A private directory, a name in it, or a copy of a package into it could not be made."""


def _user_cache_dir() -> str:
    """Return `~/Library/Caches` on macOS; elsewhere `$XDG_CACHE_HOME`, else `~/.cache`.

    A relative `$XDG_CACHE_HOME` is ignored, as the XDG spec says.
    """
    if sys.platform == "darwin":
        return os.path.expanduser("~/Library/Caches")
    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg and os.path.isabs(xdg):
        return xdg
    return os.path.expanduser("~/.cache")


def ensure_private_dir(path: str) -> None:
    """Create `path` private to this user, or check that an existing one is.

    Code is loaded from under it, so a directory another user can write to would let them plant
    code that runs in this server.
    """
    try:
        os.makedirs(path, mode=0o700, exist_ok=True)
        st = os.lstat(path)
    except OSError as e:
        raise PrivateCopyError(f"making the private directory {path} failed: {e}") from e
    if not stat.S_ISDIR(st.st_mode):
        raise PrivateCopyError(f"{path} is not a directory (a symlink?); refusing to load code from it")
    if st.st_uid != os.getuid():
        raise PrivateCopyError(
            f"{path} is owned by uid {st.st_uid}, not this user ({os.getuid()}); refusing to load code from it"
        )
    if st.st_mode & 0o022:
        raise PrivateCopyError(
            f"{path} is writable by other users (mode {stat.S_IMODE(st.st_mode):o}); "
            "refusing to load code from it. chmod go-w it."
        )


def claim_name(root: str, name: str) -> int | None:
    """Return an fd holding the lock `<root>/<name>.lock`, or None when another process holds it.

    The lock lives as long as the fd: keep it open while the name is in use. A process that holds
    a name's lock may delete the name (a launcher removing the run directory a crashed one left),
    and it may do so between our open and our lock, so the lock counts only if the path still
    names the file we locked.
    """
    path = os.path.join(root, name + ".lock")
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    except OSError as e:
        raise PrivateCopyError(f"opening the lock file {path} failed: {e}") from e
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked, named = os.fstat(fd), os.stat(path)
    except (BlockingIOError, FileNotFoundError):
        os.close(fd)
        return None
    except OSError as e:
        os.close(fd)
        raise PrivateCopyError(f"locking {path} failed: {e}") from e
    if (locked.st_dev, locked.st_ino) != (named.st_dev, named.st_ino):
        os.close(fd)
        return None
    return fd


def clone_tree(src: str, dst: str) -> None:
    """Copy the file or directory `src` to `dst`, copy-on-write where the filesystem can.

    A copy-on-write clone does not change when the source is later edited in place or replaced,
    and costs no space. Where it fails (another volume, a filesystem without clones) this logs a
    warning and makes a plain copy, which is as unaffected by later changes, only slower. Raises
    `PrivateCopyError` when that fails too; nothing is left at `dst` then.
    """
    # The source's timestamps are kept: a copied .pyc is valid only for a source of the same mtime,
    # and without them every process importing from the copy would compile the package again.
    command = ["cp", "-c", "-p", "-R"] if sys.platform == "darwin" else ["cp", "--reflink=auto", "--preserve=timestamps", "-R"]
    done = subprocess.run(command + [src, dst], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if done.returncode == 0:
        return
    clone_failure = f"{' '.join(command)} exited {done.returncode}: {done.stderr.strip()}"
    _remove_copy(dst)
    logging.getLogger("mipyre.clone").warning(
        "cloning %s into %s failed (%s); making a plain copy instead", src, dst, clone_failure
    )
    import shutil

    try:
        if os.path.isdir(src) and not os.path.islink(src):
            shutil.copytree(src, dst, symlinks=True)
        else:
            shutil.copy2(src, dst, follow_symlinks=False)
    except OSError as e:
        _remove_copy(dst)
        raise PrivateCopyError(f"copying {src} into {dst} failed: {e}; cloning it failed first ({clone_failure})") from e


def _remove_copy(path: str) -> None:
    """Remove a partial copy as what it is: a real directory whole, a file or a symlink alone."""
    import shutil

    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    elif os.path.lexists(path):
        os.remove(path)
