"""Extension binaries built in a test: just enough ELF64 / Mach-O 64 for a symbol-table reader."""

from __future__ import annotations


def elf(strtab: bytes) -> bytes:
    """An ELF64 LE file with sections [null, .dynsym (sh_link 2), .dynstr = strtab]."""
    data_off = 64
    shoff = data_off + len(strtab)
    head = bytearray(64)
    head[0:6] = b"\x7fELF\x02\x01"
    head[0x28:0x30] = shoff.to_bytes(8, "little")
    head[0x3A:0x3C] = (64).to_bytes(2, "little")
    head[0x3C:0x3E] = (3).to_bytes(2, "little")

    def section(sh_type, off, size, link):
        sh = bytearray(64)
        sh[4:8] = sh_type.to_bytes(4, "little")
        sh[24:32] = off.to_bytes(8, "little")
        sh[32:40] = size.to_bytes(8, "little")
        sh[40:44] = link.to_bytes(4, "little")
        return bytes(sh)

    return bytes(head) + strtab + section(0, 0, 0, 0) + section(11, 0, 0, 2) + section(3, data_off, len(strtab), 0)


def macho(strtab: bytes, cmdsize: int = 24) -> bytes:
    """A Mach-O 64 LE file with one load command, LC_SYMTAB, whose strings are strtab."""
    stroff = 32 + 24
    head = bytearray(32)
    head[0:4] = b"\xcf\xfa\xed\xfe"
    head[16:20] = (1).to_bytes(4, "little")  # ncmds
    cmd = (2).to_bytes(4, "little") + cmdsize.to_bytes(4, "little") + bytes(8)
    cmd += stroff.to_bytes(4, "little") + len(strtab).to_bytes(4, "little")
    return bytes(head) + cmd + strtab
