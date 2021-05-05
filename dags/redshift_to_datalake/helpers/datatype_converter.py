_conversions = {
    'bigint': 'bigint',
    'boolean': 'boolean',
    'character': 'char({character_maximum_length})',
    'character varying': 'varchar({character_maximum_length})',
    'date': 'date',
    'double precision': 'double',
    'integer': 'int',
    'numeric': 'decimal({numeric_precision}, {numeric_scale})',
    'real': 'float',
    'smallint': 'smallint',
    'text': 'string',
    'timestamp without time zone': 'timestamp'
}


def convert_datatype(datatype, character_maximum_length=None, numeric_precision=None, numeric_scale=None):
    return _conversions[datatype].format(character_maximum_length=character_maximum_length,
                                         numeric_precision=numeric_precision, numeric_scale=numeric_scale)
