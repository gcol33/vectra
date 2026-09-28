#ifndef VECTRA_PARQUET_META_H
#define VECTRA_PARQUET_META_H

/*
 * Parquet metadata: the subset of FileMetaData and PageHeader the scan uses,
 * decoded from the Thrift compact protocol (parquet_thrift.h).
 *
 * Field numbers follow parquet-format's parquet.thrift. Byte strings
 * (statistics bounds) point into the footer buffer the PqFileMeta owns, so
 * they stay valid for the life of the metadata.
 */

#include <stdint.h>
#include <stddef.h>

/* Physical types */
enum {
    PQ_BOOLEAN = 0, PQ_INT32 = 1, PQ_INT64 = 2, PQ_INT96 = 3,
    PQ_FLOAT = 4, PQ_DOUBLE = 5, PQ_BYTE_ARRAY = 6, PQ_FIXED_LEN_BYTE_ARRAY = 7
};

/* Repetition */
enum { PQ_REQUIRED = 0, PQ_OPTIONAL = 1, PQ_REPEATED = 2 };

/* Legacy ConvertedType */
enum {
    PQ_CT_UTF8 = 0, PQ_CT_MAP = 1, PQ_CT_MAP_KEY_VALUE = 2, PQ_CT_LIST = 3,
    PQ_CT_ENUM = 4, PQ_CT_DECIMAL = 5, PQ_CT_DATE = 6, PQ_CT_TIME_MILLIS = 7,
    PQ_CT_TIME_MICROS = 8, PQ_CT_TIMESTAMP_MILLIS = 9,
    PQ_CT_TIMESTAMP_MICROS = 10, PQ_CT_UINT_8 = 11, PQ_CT_UINT_16 = 12,
    PQ_CT_UINT_32 = 13, PQ_CT_UINT_64 = 14, PQ_CT_INT_8 = 15, PQ_CT_INT_16 = 16,
    PQ_CT_INT_32 = 17, PQ_CT_INT_64 = 18, PQ_CT_JSON = 19, PQ_CT_BSON = 20,
    PQ_CT_INTERVAL = 21
};

/* LogicalType union member ids */
enum {
    PQ_LT_NONE = 0, PQ_LT_STRING = 1, PQ_LT_MAP = 2, PQ_LT_LIST = 3,
    PQ_LT_ENUM = 4, PQ_LT_DECIMAL = 5, PQ_LT_DATE = 6, PQ_LT_TIME = 7,
    PQ_LT_TIMESTAMP = 8, PQ_LT_INTEGER = 10, PQ_LT_UNKNOWN = 11,
    PQ_LT_JSON = 12, PQ_LT_BSON = 13, PQ_LT_UUID = 14, PQ_LT_FLOAT16 = 15
};

/* TimeUnit */
enum { PQ_UNIT_MILLIS = 1, PQ_UNIT_MICROS = 2, PQ_UNIT_NANOS = 3 };

/* Compression codecs */
enum {
    PQ_CODEC_UNCOMPRESSED = 0, PQ_CODEC_SNAPPY = 1, PQ_CODEC_GZIP = 2,
    PQ_CODEC_LZO = 3, PQ_CODEC_BROTLI = 4, PQ_CODEC_LZ4 = 5,
    PQ_CODEC_ZSTD = 6, PQ_CODEC_LZ4_RAW = 7
};

/* Encodings */
enum {
    PQ_ENC_PLAIN = 0, PQ_ENC_PLAIN_DICTIONARY = 2, PQ_ENC_RLE = 3,
    PQ_ENC_BIT_PACKED = 4, PQ_ENC_DELTA_BINARY_PACKED = 5,
    PQ_ENC_DELTA_LENGTH_BYTE_ARRAY = 6, PQ_ENC_DELTA_BYTE_ARRAY = 7,
    PQ_ENC_RLE_DICTIONARY = 8, PQ_ENC_BYTE_STREAM_SPLIT = 9
};

/* Page types */
enum {
    PQ_PAGE_DATA = 0, PQ_PAGE_INDEX = 1, PQ_PAGE_DICTIONARY = 2,
    PQ_PAGE_DATA_V2 = 3
};

typedef struct {
    char   *name;
    int     type;            /* physical type, -1 for a group node */
    int32_t type_length;     /* FIXED_LEN_BYTE_ARRAY width */
    int     repetition;      /* PQ_REQUIRED/OPTIONAL/REPEATED; -1 = absent */
    int32_t num_children;    /* 0 for a leaf */
    int     converted_type;  /* PQ_CT_*, -1 = absent */
    int32_t scale, precision;/* legacy DECIMAL parameters */
    int     logical;         /* PQ_LT_*, PQ_LT_NONE = absent */
    int     lt_unit;         /* TIME / TIMESTAMP unit */
    int     lt_utc;          /* TIME / TIMESTAMP isAdjustedToUTC */
    int     lt_bit_width;    /* INTEGER */
    int     lt_signed;       /* INTEGER */
    int32_t lt_scale, lt_precision; /* DECIMAL */
} PqSchemaElem;

typedef struct {
    const uint8_t *min, *max;  /* plain-encoded bounds, NULL if absent */
    uint32_t       min_len, max_len;
    int            min_is_new; /* 1 = min_value/max_value (type-defined order) */
    int64_t        null_count; /* -1 if absent */
} PqStats;

typedef struct {
    int      has_meta;
    int      external;         /* file_path set: data lives in another file */
    int      type;
    int      codec;
    int64_t  num_values;
    int64_t  total_compressed_size;
    int64_t  data_page_offset;
    int64_t  dict_page_offset; /* -1 if absent */
    int      n_path;
    char   **path;             /* path_in_schema */
    PqStats  stats;
} PqColumnChunk;

typedef struct {
    PqColumnChunk *cols;
    int            n_cols;
    int64_t        num_rows;
} PqRowGroup;

typedef struct {
    uint8_t      *footer;      /* owned: the raw footer bytes */
    PqSchemaElem *schema;
    int           n_schema;
    int64_t       num_rows;
    PqRowGroup   *rgs;
    int           n_rgs;
} PqFileMeta;

typedef struct {
    int      type;
    int32_t  uncompressed_size;
    int32_t  compressed_size;
    int      has_data, has_dict, has_v2;
    /* DATA_PAGE / DATA_PAGE_V2 / DICTIONARY_PAGE */
    int32_t  num_values;
    int      encoding;
    /* DATA_PAGE (v1) */
    int      def_encoding, rep_encoding;
    /* DATA_PAGE_V2 */
    int32_t  num_nulls, num_rows;
    int32_t  def_len, rep_len;
    int      is_compressed;
} PqPageHeader;

/* Decode FileMetaData from footer bytes. Takes ownership of `footer` (freed
   by pq_meta_free, also on failure). Returns 0 on success, -1 with a message
   in err on malformed input. */
int pq_meta_decode(uint8_t *footer, size_t len, PqFileMeta *out,
                   char *err, size_t errlen);

void pq_meta_free(PqFileMeta *m);

/* Decode a page header. Returns the number of header bytes consumed, or -1
   on malformed input. */
int64_t pq_page_header_decode(const uint8_t *p, size_t n, PqPageHeader *h);

#endif /* VECTRA_PARQUET_META_H */
