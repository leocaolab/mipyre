"""File upload support — multipart/form-data parser.

Usage::

    from mipyre.uploads import parse_multipart

    @app.post("/upload")
    def upload(req):
        form = parse_multipart(req)
        f = form["file"]
        return {"filename": f.filename, "size": len(f.data)}

A field name sent more than once reads as its first part (``form["tag"]``,
``form.get("tag")``), as ``req.query_params`` reads a repeated key;
``form.get_all("tag")`` returns every part in the order sent.

``FormPart.filename`` is what the client sent, decoded but otherwise as is: it
may hold ``../``, an absolute path, a drive letter, control characters or be
empty. Never join it into a filesystem path; name stored files yourself (a
UUID, a content hash) and keep the client's name only as data.
"""

from __future__ import annotations
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from urllib.parse import unquote_to_bytes

from mipyre.engine import HTTPException


def _split_header_params(value: str) -> list[str]:
    """Split a header on top-level ``;`` separators; a ``;`` inside an RFC 2045
    quoted-string (``filename="report;2024.csv"``) is literal."""
    if '"' not in value:
        return value.split(";")
    parts: list[str] = []
    buf: list[str] = []
    in_quotes = False
    escaped = False
    for ch in value:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if in_quotes and ch == "\\":
            buf.append(ch)
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == ";" and not in_quotes:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return parts


def _unquote_param(value: str) -> str:
    """Strip surrounding DQUOTEs and unescape ``\\"`` / ``\\\\`` per RFC 2045
    quoted-string rules."""
    value = value.strip()
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        inner = value[1:-1]
        out: list[str] = []
        escaped = False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        # A trailing backslash (malformed quoted-string per RFC 2045) leaves
        # `escaped` set with nothing to escape — preserve it as a literal
        # rather than silently dropping it.
        if escaped:
            out.append("\\")
        return "".join(out)
    return value


@dataclass(frozen=True, slots=True)
class FormPart:
    """One part of a multipart/form-data body: an uploaded file, or a field (``filename`` None)."""
    name: str
    # Client-controlled: see the module docstring before using it in a path.
    filename: str | None
    content_type: str
    data: bytes

    @property
    def text(self) -> str:
        # Lossy on purpose: an upload may be any bytes. Strict decoding: use `.data`.
        return self.data.decode("utf-8", errors="replace")

    @property
    def size(self) -> int:
        return len(self.data)


# Longest run of transport padding (spaces/tabs) read after a delimiter when
# telling CRLF from LF framing.
_MAX_PADDING = 64


class MultipartError(HTTPException):
    """The request body is not well-formed multipart/form-data (400), or is beyond the
    limits ``parse_multipart`` was given (413). An ``HTTPException``: left uncaught, the
    client gets that status and the reason as ``detail``."""


class Form(Mapping[str, FormPart]):
    """The parts of a multipart/form-data body, by field name.

    A name sent more than once reads as its first part; ``get_all`` returns every part
    of a name in the order sent.
    """

    __slots__ = ("_parts",)

    def __init__(self, parts: dict[str, list[FormPart]]) -> None:
        self._parts = parts

    def __getitem__(self, name: str) -> FormPart:
        return self._parts[name][0]

    def __iter__(self) -> Iterator[str]:
        return iter(self._parts)

    def __len__(self) -> int:
        return len(self._parts)

    def __eq__(self, other: object) -> bool:
        # Mapping's equality compares first parts only and would call two forms equal
        # that differ in a repeated name.
        if isinstance(other, Form):
            return self._parts == other._parts
        return NotImplemented

    def get_all(self, name: str) -> list[FormPart]:
        """Every part named ``name``, in the order sent; ``[]`` if none."""
        return list(self._parts.get(name, ()))

    def __repr__(self) -> str:
        return f"Form({self._parts!r})"


# CVE-2023-30798 is a body of many tiny parts; 1000 is Starlette's and Werkzeug's cap.
def parse_multipart(req, *, max_parts: int = 1000, max_part_header_bytes: int = 16 * 1024) -> Form:
    """Parse multipart/form-data from request. A text field's ``filename`` is None.

    Raises MultipartError (an ``HTTPException``) when the body is not well-formed
    multipart/form-data (400), has more than ``max_parts`` parts, or a part whose header
    section is longer than ``max_part_header_bytes`` (413). Both limits are checked
    before the part is parsed.
    """
    if max_parts < 1 or max_part_header_bytes < 1:
        raise ValueError(
            f"max_parts={max_parts} and max_part_header_bytes={max_part_header_bytes} "
            "must both be at least 1"
        )
    boundary = _boundary(req.headers.get("content-type", ""))
    body = req.body
    nl = _line_break(body, boundary)
    parts: dict[str, list[FormPart]] = {}
    for part in _split_parts(body, boundary, nl, max_parts):
        upload = _parse_part(part, nl, max_part_header_bytes)
        parts.setdefault(upload.name, []).append(upload)
    return Form(parts)


def _boundary(content_type: str) -> str:
    media_type, *params = _split_header_params(content_type)
    # RFC 9110 §8.3.1: the type, subtype and parameter names are case-insensitive; the
    # boundary value is not. A quoted boundary may contain ";".
    if media_type.strip().lower() != "multipart/form-data":
        raise MultipartError(400, f"Expected multipart/form-data, got: {content_type}")
    boundary = _header_params(params).get("boundary")
    if not boundary:
        raise MultipartError(400, f"Missing boundary in Content-Type: {content_type}")
    return boundary


def _header_params(params: list[str]) -> dict[str, str]:
    """``name=value`` header parameters by lowercased name, values unquoted."""
    out: dict[str, str] = {}
    for param in params:
        name, eq, value = param.partition("=")
        if eq:
            out[name.strip().lower()] = _unquote_param(value)
    return out


# RFC 8187 §3.2.1: recipients must support these two charsets.
_EXT_VALUE_CHARSETS = {"utf-8": "utf-8", "iso-8859-1": "latin-1"}


def _ext_value(value: str) -> str:
    """An RFC 8187 (RFC 5987) ext-value, ``charset'language'pct-encoded``, decoded."""
    charset, sep1, rest = value.partition("'")
    _language, sep2, encoded = rest.partition("'")
    codec = _EXT_VALUE_CHARSETS.get(charset.strip().lower())
    if not (sep1 and sep2) or codec is None:
        raise MultipartError(
            400,
            f"filename*={value!r} is not charset'language'value with charset UTF-8 or "
            "ISO-8859-1 (RFC 8187)"
        )
    try:
        return unquote_to_bytes(encoded).decode(codec)
    except UnicodeDecodeError as e:
        raise MultipartError(400, f"filename*={value!r} is not valid {charset}: {e}") from None


def _line_break(body: bytes, boundary: str) -> bytes:
    """The line break the body is framed with, read off the first delimiter line:
    CRLF per RFC 2046, or bare LF from clients/proxies that strip CR."""
    dash = b"--" + boundary.encode()
    start = body.find(dash)
    if start == -1:
        raise MultipartError(400, f"no delimiter line --{boundary} in the body")
    line_end = body[start + len(dash):start + len(dash) + _MAX_PADDING].lstrip(b" \t")
    return b"\r\n" if line_end.startswith(b"\r\n") else b"\n"


def _split_parts(body: bytes, boundary: str, nl: bytes, max_parts: int) -> list[bytes]:
    """The content of every part, headers included.

    RFC 2046 §5.1.1: a delimiter is ``CRLF--boundary``; the CRLF belongs to the
    delimiter, not to the part before it, so a part's content ends right where
    the delimiter begins. The body is prefixed with a line break so the first
    delimiter (at the very start, with no line break before it) is anchored like
    the rest.

    The split stops after ``max_parts + 1`` delimiters, so a body of many tiny parts
    costs no more than ``max_parts`` of them before it is refused.
    """
    segments = (nl + body).split(nl + b"--" + boundary.encode(), max_parts + 1)
    if len(segments) < 2:
        raise MultipartError(400, f"no delimiter line --{boundary} in the body")

    parts: list[bytes] = []
    for segment in segments[1:]:  # segments[0] is the preamble
        if segment.startswith(b"--"):
            return parts
        if len(parts) == max_parts:
            raise MultipartError(413, f"body has more than max_parts={max_parts} parts")
        padding, sep, part = segment.partition(nl)
        if not sep or padding.strip(b" \t"):
            raise MultipartError(
                400,
                f"delimiter --{boundary} is followed by {padding[:40]!r}, not a line break"
            )
        parts.append(part)
    raise MultipartError(400, f"body ends without the closing delimiter --{boundary}--")


def _parse_part(part: bytes, nl: bytes, max_header_bytes: int) -> FormPart:
    # A part with no header lines starts with the blank line itself.
    header_section, sep, data = (
        (b"", nl, part[len(nl):]) if part.startswith(nl) else part.partition(nl + nl)
    )
    if not sep:
        raise MultipartError(400, f"part has no blank line after its headers: {part[:80]!r}")
    if len(header_section) > max_header_bytes:
        raise MultipartError(
            413,
            f"part's header section is {len(header_section)} bytes, more than "
            f"max_part_header_bytes={max_header_bytes}: {header_section[:80]!r}"
        )

    headers = {}
    for line in header_section.decode("utf-8", errors="replace").split("\n"):
        key, colon, val = line.strip().partition(":")
        if colon:
            headers[key.strip().lower()] = val.strip()

    # RFC 2045 §5.1: parameter names are case-insensitive (NAME=, FileName=),
    # and quoted values may contain semicolons and escaped quotes. RFC 6266 §4.3:
    # filename* (RFC 8187, non-ASCII names) wins over filename.
    disposition = headers.get("content-disposition", "")
    params = _header_params(_split_header_params(disposition)[1:])
    field_name = params.get("name")
    filename = _ext_value(params["filename*"]) if "filename*" in params else params.get("filename")
    if not field_name:
        raise MultipartError(
            400,
            f"part has no field name in its Content-Disposition: {disposition!r}"
        )

    default_type = "application/octet-stream" if filename else "text/plain"
    return FormPart(
        name=field_name,
        filename=filename,
        content_type=headers.get("content-type", default_type),
        data=data,
    )
