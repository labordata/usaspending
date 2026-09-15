"""Pull one table out of USAspending's monthly PostgreSQL dump without
downloading the dump.

    python scripts/dump_extract.py usaspending-db_20260906.zip rpt.subaward_search out.tsv

The dump is a zip of a pg_dump directory-format archive: toc.dat plus one
NNNN.dat.gz per table, each *stored* (not deflated) in the zip. So a table
is a gzip file sitting at a known byte offset inside a 178 GB file, and HTTP
range requests can fetch just that: the zip central directory (from the
end of the file) gives the member's offset and size; toc.dat maps table name
to member and carries the column list from its COPY statement.

The server drops long-lived connections, so the member is fetched in
resumable chunks. Output is PostgreSQL COPY text: tab-delimited, \\N for
NULL, backslash escapes, no header -- the column names are written to
<out>.columns for the loader.
"""
import argparse
import gzip
import io
import os
import re
import struct
import subprocess
import sys
import tempfile
import time
import zipfile

import requests

BASE = "https://files.usaspending.gov/database_download/"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/128.0.0.0 Safari/537.36"
CHUNK = 32 << 20


def fetch_range(url, start, end, retries=8):
    """Bytes [start, end] inclusive, retrying on the server's dropped connections."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Range": f"bytes={start}-{end}"}, timeout=120)
            if r.status_code != 206:
                raise IOError(f"expected 206, got {r.status_code}")
            if len(r.content) != end - start + 1:
                raise IOError(f"short read: {len(r.content)} of {end - start + 1}")
            return r.content
        except (requests.RequestException, IOError) as e:
            wait = 2 ** attempt
            print(f"    range {start}-{end}: {e}; retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise IOError(f"gave up on range {start}-{end}")


def central_directory(url, size):
    """Parse the zip64 central directory from the tail of the remote file."""
    tail_len = min(size, 1 << 20)
    tail = fetch_range(url, size - tail_len, size - 1)
    # zip64 end-of-central-directory locator -> EOCD64 record -> directory offset/size
    loc = tail.rfind(b"PK\x06\x07")
    if loc < 0:
        raise IOError("no zip64 locator; not a zip64 archive?")
    eocd64_off = struct.unpack("<Q", tail[loc + 8:loc + 16])[0]
    eocd64 = fetch_range(url, eocd64_off, eocd64_off + 55)
    cd_size, cd_off = struct.unpack("<QQ", eocd64[40:56])
    cd = fetch_range(url, cd_off, cd_off + cd_size - 1)
    members = {}
    pos = 0
    while pos < len(cd) and cd[pos:pos + 4] == b"PK\x01\x02":
        rec = struct.unpack(zipfile.structCentralDir, cd[pos:pos + zipfile.sizeCentralDir])
        comp, csize, usize, nlen, xlen, clen, lho = rec[6], rec[10], rec[11], rec[12], rec[13], rec[14], rec[18]
        name = cd[pos + 46:pos + 46 + nlen].decode()
        extra = cd[pos + 46 + nlen:pos + 46 + nlen + xlen]
        # zip64 extra field supplies the 64-bit values for any field set to 0xFFFFFFFF
        if 0xFFFFFFFF in (csize, usize, lho):
            i = 0
            while i < len(extra):
                hid, hlen = struct.unpack("<HH", extra[i:i + 4])
                if hid == 1:
                    vals = struct.unpack("<" + "Q" * (hlen // 8), extra[i + 4:i + 4 + hlen])
                    vals = list(vals)
                    if usize == 0xFFFFFFFF: usize = vals.pop(0)
                    if csize == 0xFFFFFFFF: csize = vals.pop(0)
                    if lho == 0xFFFFFFFF: lho = vals.pop(0)
                i += 4 + hlen
        members[name] = (comp, csize, usize, lho)
        pos += 46 + nlen + xlen + clen
    return members


def member_data_offset(url, lho):
    """Local file header -> where the member's bytes actually start."""
    h = fetch_range(url, lho, lho + 29)
    nlen, xlen = struct.unpack("<HH", h[26:30])
    return lho + 30 + nlen + xlen


def trim_copy_terminator(path):
    """Drop the trailing "\\." line (and any blank lines after it)."""
    with open(path, "rb+") as o:
        o.seek(0, os.SEEK_END)
        size = o.tell()
        o.seek(max(0, size - 64))
        tail = o.read()
        body = tail.rstrip(b"\n")
        if body.endswith(b"\\."):
            o.truncate(size - len(tail) + len(body) - 2)


def latest_dump(days=60):
    """Name of the most recent usaspending-db_YYYYMMDD.zip, probing backwards."""
    import datetime
    today = datetime.date.today()
    for i in range(days):
        d = today - datetime.timedelta(days=i)
        name = f"usaspending-db_{d:%Y%m%d}.zip"
        r = requests.head(BASE + name, headers={"User-Agent": UA}, timeout=60)
        if r.status_code == 200:
            return name
        time.sleep(0.5)
    raise IOError(f"no dump found in the last {days} days")


class Dump:
    """One monthly dump: central directory + TOC read once, tables fetched on demand."""

    def __init__(self, name):
        self.name = name
        self.url = BASE + name
        self.size = int(requests.head(self.url, headers={"User-Agent": UA}).headers["Content-Length"])
        self.members = central_directory(self.url, self.size)
        comp, csize, usize, lho = self.members["toc.dat"]
        off = member_data_offset(self.url, lho)
        toc = fetch_range(self.url, off, off + csize - 1)
        if comp == 8:
            toc = zipfile.zlib.decompress(toc, -15)
        self.toc = toc
        # pg_restore -l reads the archive TOC properly (needs a pg_restore at
        # least as new as the dump's format; 18 handles today's 1.15). It
        # gives dump id -> file. Column lists come from the COPY statements
        # in toc.dat, which pg_restore -l does not print.
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, "toc.dat"), "wb").write(toc)
            self.listing = subprocess.run(["pg_restore", "-l", d], check=True,
                                          capture_output=True, text=True).stdout

    def table(self, qualified):
        schema, table = qualified.split(".")
        m = re.search(rf"^(\d+); \d+ \d+ TABLE DATA {schema} {table} ", self.listing, re.M)
        if not m:
            raise KeyError(f"{qualified} not in {self.name}")
        cm = re.search(rb'COPY "?%s"?\.%s \((.*?)\) FROM stdin;' % (schema.encode(), table.encode()), self.toc)
        # pg_dump double-quotes column names that are reserved words ("authorization")
        columns = [c.strip('"') for c in cm.group(1).decode().split(", ")]
        return m.group(1) + ".dat.gz", columns

    def fetch_table(self, qualified, out_gz):
        """Range-fetch one table's gzip into out_gz (resumable). Returns its column names."""
        fname, columns = self.table(qualified)
        comp, csize, usize, lho = self.members[fname]
        if comp != 0:
            raise IOError(f"{fname} is deflated inside the zip (method {comp}); expected stored")
        start = member_data_offset(self.url, lho)
        print(f"{qualified} -> {fname}: {csize/2**20:.0f} MB gzip", file=sys.stderr)
        done = os.path.getsize(out_gz) if os.path.exists(out_gz) else 0
        with open(out_gz, "ab") as f:
            while done < csize:
                end = min(done + CHUNK, csize) - 1
                f.write(fetch_range(self.url, start + done, start + end))
                done = end + 1
                print(f"  {done/2**20:7.0f} / {csize/2**20:.0f} MB", file=sys.stderr, end="\r")
        print(file=sys.stderr)
        return columns


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump", help="e.g. usaspending-db_20260906.zip, or 'latest'")
    ap.add_argument("table", help="schema.table, e.g. rpt.subaward_search")
    ap.add_argument("out", help="output .tsv (decompressed, COPY terminator removed)")
    args = ap.parse_args()
    dump = Dump(latest_dump() if args.dump == "latest" else args.dump)
    print(f"{dump.name}: {dump.size/2**30:.0f} GiB, {len(dump.members)} members", file=sys.stderr)
    columns = dump.fetch_table(args.table, args.out + ".gz")
    with open(args.out + ".columns", "w") as f:
        f.write("\n".join(columns) + "\n")
    with gzip.open(args.out + ".gz", "rb") as g, open(args.out, "wb") as o:
        while chunk := g.read(1 << 20):
            o.write(chunk)
    os.remove(args.out + ".gz")
    trim_copy_terminator(args.out)
    print(f"{args.out}: {os.path.getsize(args.out)/2**20:.0f} MB, {len(columns)} columns", file=sys.stderr)


if __name__ == "__main__":
    main()
