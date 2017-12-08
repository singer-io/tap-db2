"""Contains functionality for converting DB2 catalog results into
Singer schemas."""
from singer.catalog import Schema

# https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzch2num.htm
BYTES_FOR_INTEGER_TYPE = {
    "smallint": 2,
    "integer": 4,
    "bigint": 8,
}
FLOAT_TYPES = {
    "float",
    "decfloat",
}
DECIMAL_TYPES = {
    "decimal",
    "numeric",
}

# https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcharstrg.htm
STRING_TYPES = {
    "char",
    "varchar",
    "time",
}

# https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzch2datetime.htm
DATETIME_TYPES = {
    "date",
    "timestmp",
}

# Parent article for data types:
# https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzch2data.htm
def _for_column(col):
    data_type = col.data_type.lower()
    inclusion = "available"
    # We want to automatically include all primary key columns
    # if col.column_key.lower() == "pri":
    #     inclusion = "automatic"
    result = Schema(inclusion=inclusion)
    if data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ["null", "integer"]
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1
    elif data_type in FLOAT_TYPES:
        result.type = ["null", "number"]
    elif data_type in DECIMAL_TYPES:
        result.type = ["null", "number"]
        result.exclusiveMaximum = True
        result.maximum = 10 ** (col.numeric_precision - col.numeric_scale)
        result.exclusiveMinimum = True
        result.minimum = -10 ** (col.numeric_precision - col.numeric_scale)
        result.multipleOf = 10 ** (0 - col.numeric_scale)
    elif data_type in STRING_TYPES:
        result.type = ["null", "string"]
        result.maxLength = col.character_maximum_length
    elif data_type in DATETIME_TYPES:
        result.type = ["null", "string"]
        result.format = "date-time"
    else:
        err = "Unsupported data type {}".format(data_type)
        result = Schema(None, inclusion="unsupported", description=err)
        with open("unsupported.txt", "a") as f:
            f.write(err + "\n")
    return result


def generate(columns):
    properties = {c.column_name: _for_column(c) for c in columns}
    return Schema(type="object", selected=False, properties=properties)
