"""Writes the Parquet fixtures read by tests/testthat/test-parquet.R.

Run from this directory with pyarrow installed:  python make_fixtures.py

Every column is a closed-form function of the row index, mirrored by
pq_expected() in test-parquet.R, so the tests compare against values the R
side computes itself and need no Arrow installation.
"""

import datetime
import decimal
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

N = 300


def col(f, null=None, n=N):
    return [None if (null is not None and null(i)) else f(i) for i in range(n)]


def base_table(n=N):
    epoch = datetime.date(1970, 1, 1)
    cols = {
        "id": pa.array(range(n), pa.int32()),
        "b": pa.array(col(lambda i: i % 3 == 0, lambda i: i % 11 == 0, n), pa.bool_()),
        "i8": pa.array(col(lambda i: (i * 37) % 256 - 128, lambda i: i % 7 == 0, n), pa.int8()),
        "i16": pa.array(col(lambda i: (i * 1231) % 65536 - 32768, lambda i: i % 13 == 0, n), pa.int16()),
        "i32": pa.array(col(lambda i: (i * 2654435761) % 2**32 - 2**31, lambda i: i % 5 == 0, n), pa.int32()),
        "i64": pa.array(col(lambda i: i * i * 7919 - 3000000000 * (i % 2), lambda i: i % 17 == 0, n), pa.int64()),
        "u8": pa.array(col(lambda i: (i * 13) % 256, None, n), pa.uint8()),
        "u16": pa.array(col(lambda i: (i * 997) % 65536, None, n), pa.uint16()),
        "u32": pa.array(col(lambda i: (i * 2654435761) % 2**32, lambda i: i % 19 == 0, n), pa.uint32()),
        "u64": pa.array(col(lambda i: i * 10**12, None, n), pa.uint64()),
        "f32": pa.array(col(lambda i: i / 8 - 50, lambda i: i % 9 == 0, n), pa.float32()),
        "f64": pa.array(col(lambda i: i * 0.37 - 100.0, lambda i: i % 10 == 0, n), pa.float64()),
        "f16": pa.array(np.array(col(lambda i: (i % 100) / 4, None, n), dtype=np.float16), pa.float16()),
        "s": pa.array(col(lambda i: "" if i % 31 == 0 else
                          ("été" if i % 29 == 0 else "s%d%s" % (i % 37, "_" * (i % 5))),
                          lambda i: i % 6 == 0, n), pa.string()),
        "s_long": pa.array(col(lambda i: "prefix_%05d_%d" % (i // 10, i), None, n), pa.string()),
        "d": pa.array(col(lambda i: epoch + datetime.timedelta(days=i * 3 - 1000),
                          lambda i: i % 8 == 0, n), pa.date32()),
        "ts_ms": pa.array(col(lambda i: 1600000000000 + i * 86400123, lambda i: i % 12 == 0, n),
                          pa.timestamp("ms", tz="UTC")),
        "ts_us": pa.array(col(lambda i: 1600000000000000 + i * 3600000001, None, n),
                          pa.timestamp("us")),
        "ts_ns": pa.array(col(lambda i: 1600000000000000000 + i * 1000000000, None, n),
                          pa.timestamp("ns", tz="UTC")),
        "t_ms": pa.array(col(lambda i: (i * 1000003) % 86400000, None, n), pa.time32("ms")),
        "t_us": pa.array(col(lambda i: (i * 1000000007) % 86400000000, None, n), pa.time64("us")),
        "dec": pa.array(col(lambda i: decimal.Decimal(i * 123 - 5000) / 100, lambda i: i % 14 == 0, n),
                        pa.decimal128(10, 2)),
        "dec_big": pa.array(col(lambda i: decimal.Decimal(i * 1000003 - 7) / 1000, None, n),
                            pa.decimal128(30, 3)),
        "uuid": pa.ExtensionArray.from_storage(pa.uuid(), pa.array(
            col(lambda i: bytes(((i * 7 + k) % 256) for k in range(16)), None, n), pa.binary(16))),
        "code": pa.array(col(lambda i: ("A%02d" % (i % 100)).encode(), lambda i: i % 15 == 0, n),
                         pa.binary(3)),
        "tags": pa.array(col(lambda i: [] if i % 9 == 1 else
                             ["t%d" % (i % 4)] + ([None] if i % 5 == 0 else ["u%d" % (i % 3)]),
                             lambda i: i % 9 == 0, n), pa.list_(pa.string())),
        "nums": pa.array(col(lambda i: list(range(i % 4)), lambda i: i % 10 == 3, n),
                         pa.list_(pa.int32())),
        "st": pa.array(col(lambda i: {"a": i * 2, "b": None if i % 4 == 0 else "b%d" % i},
                           lambda i: i % 7 == 3, n),
                       pa.struct([("a", pa.int32()), ("b", pa.string())])),
    }
    return pa.table(cols)


def write(name, table, **kw):
    kw.setdefault("row_group_size", 100)
    kw.setdefault("data_page_size", 512)
    kw.setdefault("write_batch_size", 64)
    pq.write_table(table, name, **kw)


def main():
    t = base_table()
    delta = {"id": "DELTA_BINARY_PACKED", "i32": "DELTA_BINARY_PACKED",
             "i64": "DELTA_BINARY_PACKED", "u32": "DELTA_BINARY_PACKED",
             "s": "DELTA_LENGTH_BYTE_ARRAY", "s_long": "DELTA_BYTE_ARRAY",
             "uuid": "DELTA_BYTE_ARRAY", "f32": "BYTE_STREAM_SPLIT",
             "f64": "BYTE_STREAM_SPLIT", "b": "RLE"}
    write("plain_none_v1.parquet", t, compression="none", use_dictionary=False,
          data_page_version="1.0")
    write("dict_snappy_v2.parquet", t, compression="snappy", use_dictionary=True,
          data_page_version="2.0")
    write("dict_lz4_v1.parquet", t, compression="lz4", use_dictionary=True,
          data_page_version="1.0")
    write("delta_gzip_v1.parquet", t, compression="gzip", use_dictionary=False,
          column_encoding=delta, data_page_version="1.0")
    write("delta_zstd_v2.parquet", t, compression="zstd", use_dictionary=False,
          column_encoding=delta, data_page_version="2.0")
    write("plain_zstd_v2.parquet", t, compression="zstd", use_dictionary=False,
          data_page_version="2.0", store_decimal_as_integer=True,
          use_deprecated_int96_timestamps=True)

    nested = pa.table({
        "id": pa.array(range(10), pa.int32()),
        "deep": pa.array([[[i, i + 1], []] for i in range(10)], pa.list_(pa.list_(pa.int32()))),
        "m": pa.array([[("k%d" % i, i)] for i in range(10)], pa.map_(pa.string(), pa.int32())),
    })
    write("nested.parquet", nested, compression="snappy")

    os.makedirs("dataset/part=a", exist_ok=True)
    os.makedirs("dataset/part=b", exist_ok=True)
    write("dataset/part=a/000.parquet", t.slice(0, 120), compression="snappy")
    write("dataset/part=b/001.parquet", t.slice(120, 180), compression="zstd",
          data_page_version="2.0")
    with open("dataset/_SUCCESS", "w"):
        pass


if __name__ == "__main__":
    main()
