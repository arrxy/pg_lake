/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Functions for generating query for writing data via pgduck server.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/parquet/geoparquet.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/util/numeric.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/parse_struct.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

static char *TupleDescToProjectionListForWrite(TupleDesc tupleDesc,
											   CopyDataFormat destinationFormat);
static DuckDBTypeInfo ChooseDuckDBEngineTypeForWrite(PGType postgresType,
													 CopyDataFormat destinationFormat);
static void AppendFieldIdValue(StringInfo map, Field * field, int fieldId);
static const char *ParquetVersionToString(ParquetVersion version);
static char *WrapQueryWithIcebergTemporalValidation(char *query,
													TupleDesc tupleDesc);
static const char *GetTemporalMinLiteral(Oid typeOid);
static const char *GetTemporalMaxLiteral(Oid typeOid);
static char *WrapQueryWithIcebergNumericValidation(char *query,
												   TupleDesc tupleDesc);
static char *NeutralizeNumericCastsInQuery(char *query);
static const char *GetNumericMaxLiteral(int precision, int scale);
static void AppendNumericNaNAction(StringInfo buf, const char *expr,
								   int precision, int scale);
static void AppendNumericOutOfRangeAction(StringInfo buf, const char *expr,
										  int precision, int scale,
										  const char *errMsg);
static void AppendNumericValidationCaseExpr(StringInfo buf, const char *expr,
											int typmod);
static void AppendNumericValidationExpr(StringInfo buf, const char *expr,
										const char *alias, int typmod);
static void AppendNumericArrayValidationExpr(StringInfo buf, const char *expr,
											 const char *alias, int typmod);
static bool IsNumericArrayType(Oid typeOid);

static DuckDBTypeInfo VARCHAR_TYPE =
{
	DUCKDB_TYPE_VARCHAR, false, "VARCHAR",
};

int			TargetRowGroupSizeMB = DEFAULT_TARGET_ROW_GROUP_SIZE_MB;
int			DefaultParquetVersion = PARQUET_VERSION_V1;
int			IcebergOutOfRangeValues = ICEBERG_OUT_OF_RANGE_ERROR;


/*
 * ConvertCSVFileTo copies and converts a CSV file at source path to
 * the destinationPath.
 *
 * The CSV was generated using COPY ... TO '<csvFilePath>'
 */
StatsCollector *
ConvertCSVFileTo(char *csvFilePath, TupleDesc csvTupleDesc, int maxLineSize,
				 char *destinationPath,
				 CopyDataFormat destinationFormat,
				 CopyDataCompression destinationCompression,
				 List *formatOptions,
				 DataFileSchema * schema,
				 List *leafFields)
{
	StringInfoData command;

	initStringInfo(&command);

	/* project columns into target format */
	appendStringInfo(&command, "SELECT %s FROM ",
					 TupleDescToProjectionListForWrite(csvTupleDesc, destinationFormat));

	/* build the read_csv(...) clause */
	char	   *columnsMap = NULL;

	if (csvTupleDesc != NULL && csvTupleDesc->natts > 0)
		columnsMap = TupleDescToColumnMapForWrite(csvTupleDesc, destinationFormat);

	bool		includeHeader = true;

	AppendReadCSVClause(&command, csvFilePath, maxLineSize, columnsMap,
						InternalCSVOptions(includeHeader));

	bool		queryHasRowIds = false;

	return WriteQueryResultTo(command.data,
							  destinationPath,
							  destinationFormat,
							  destinationCompression,
							  formatOptions,
							  queryHasRowIds,
							  schema,
							  csvTupleDesc,
							  leafFields);
}


/*
 * WriteQueryResultTo takes the result of a query and writes to
 * destinationPath. There may be multiple files if file_size_bytes
 * is specified in formatOptions.
 */
StatsCollector *
WriteQueryResultTo(char *query,
				   char *destinationPath,
				   CopyDataFormat destinationFormat,
				   CopyDataCompression destinationCompression,
				   List *formatOptions,
				   bool queryHasRowId,
				   DataFileSchema * schema,
				   TupleDesc queryTupleDesc,
				   List *leafFields)
{
	/*
	 * For Iceberg, wrap the query with temporal range validation.
	 *
	 * All writes (direct INSERT, INSERT..SELECT, COPY FROM) flow through
	 * here.  This wrapper ensures out-of-range temporal values are rejected
	 * at the DuckDB level before being written to Parquet files.
	 */
	if (destinationFormat == DATA_FORMAT_ICEBERG)
	{
		query = WrapQueryWithIcebergTemporalValidation(query, queryTupleDesc);
		query = WrapQueryWithIcebergNumericValidation(query, queryTupleDesc);
	}

	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "COPY (%s) TO %s",
					 query,
					 quote_literal_cstr(destinationPath));

	/* start WITH options */
	appendStringInfoString(&command, " WITH (");

	/*
	 * Iceberg data files are Parquet, so use "parquet" as the DuckDB format
	 * name for both DATA_FORMAT_PARQUET and DATA_FORMAT_ICEBERG.
	 */
	const char *formatName = (destinationFormat == DATA_FORMAT_ICEBERG) ?
		"parquet" : CopyDataFormatToName(destinationFormat);

	appendStringInfo(&command, "format %s",
					 quote_literal_cstr(formatName));

	switch (destinationFormat)
	{
		case DATA_FORMAT_ICEBERG:
		case DATA_FORMAT_PARQUET:
			{
				if (destinationCompression == DATA_COMPRESSION_NONE)
				{
					/* Parquet format uses uncompressed instead of none */
					appendStringInfo(&command, ", compression 'uncompressed'");
					break;
				}
				else
				{

					const char *compressionName =
						CopyDataCompressionToName(destinationCompression);

					appendStringInfo(&command, ", compression %s",
									 quote_literal_cstr(compressionName));
				}

				ListCell   *optionCell = NULL;

				foreach(optionCell, formatOptions)
				{
					DefElem    *option = lfirst(optionCell);

					if (strcmp(option->defname, "file_size_bytes") == 0)
					{
						char	   *fileSizeStr = defGetString(option);

						appendStringInfo(&command, ", file_size_bytes %s",
										 quote_literal_cstr(fileSizeStr));
					}
				}

				if (schema != NULL)
				{
					appendStringInfoString(&command, ", field_ids {");
					AppendFields(&command, schema);

					if (queryHasRowId)
						appendStringInfo(&command, ", '_row_id' : %d", ICEBERG_ROWID_FIELD_ID);

					appendStringInfoString(&command, "}");
				}

				if (queryTupleDesc != NULL)
				{
					char	   *geoParquetMeta =
						GetGeoParquetMetadataForTupleDesc(queryTupleDesc);

					if (geoParquetMeta != NULL)
					{
						appendStringInfo(&command, ", kv_metadata { geo: %s }",
										 quote_literal_cstr(geoParquetMeta));
					}
				}

				if (TargetRowGroupSizeMB > 0)
				{
					/*
					 * When writing Parquet files, a single row group per
					 * thread must fit in memory uncompressed. Hence, set
					 * row_group_size_bytes to 128MB.
					 * https://github.com/duckdb/duckdb/issues/16078#issuecomment-2644985411
					 *
					 * duckdb also uses row_group_size which is set to 122880
					 * rows by default. If row_group_size hits the limit
					 * before row_group_size_bytes, it will be used instead.
					 *
					 * row_group_size_bytes also requires
					 * preserve_insertion_order=false.
					 */
					appendStringInfo(&command, ", row_group_size_bytes '%dMB'", TargetRowGroupSizeMB);
				}

				appendStringInfo(&command, ", parquet_version '%s'",
								 ParquetVersionToString(DefaultParquetVersion));

				appendStringInfo(&command, ", return_stats");

				break;
			}

		case DATA_FORMAT_JSON:
			{
				if (destinationCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is not "
										   "supported for JSON format")));
				}

				const char *compressionName =
					CopyDataCompressionToName(destinationCompression);

				appendStringInfo(&command, ", compression %s",
								 quote_literal_cstr(compressionName));
				break;
			}

		case DATA_FORMAT_CSV:
			{
				if (destinationCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is not "
										   "supported for CSV format")));
				}

				const char *compressionName =
					CopyDataCompressionToName(destinationCompression);

				appendStringInfo(&command, ", compression %s",
								 quote_literal_cstr(compressionName));

				/*
				 * We normalize the list of options to include default values
				 * for all options, unless auto_detect is on, in which case we
				 * only include the explicitly defined ones.
				 */
				List	   *csvOptions = NormalizedExternalCSVOptions(formatOptions);

				ListCell   *optionCell = NULL;

				foreach(optionCell, csvOptions)
				{
					DefElem    *option = lfirst(optionCell);

					if (strcmp(option->defname, "header") == 0)
					{
						CopyHeaderChoice choice =
							GetCopyHeaderChoice(option, true);

						appendStringInfo(&command, ", header %s",
										 choice == COPY_HEADER_FALSE ? "false" : "true");
					}
					else if (strcmp(option->defname, "delimiter") == 0)
					{
						char	   *delimiter = defGetString(option);

						appendStringInfo(&command, ", delim %s",
										 quote_literal_cstr(delimiter));
					}
					else if (strcmp(option->defname, "quote") == 0)
					{
						char	   *quote = defGetString(option);

						appendStringInfo(&command, ", quote %s",
										 quote_literal_cstr(quote));
					}
					else if (strcmp(option->defname, "escape") == 0)
					{
						char	   *escape = defGetString(option);

						appendStringInfo(&command, ", escape %s",
										 quote_literal_cstr(escape));
					}
					else if (strcmp(option->defname, "null") == 0)
					{
						char	   *null = defGetString(option);

						appendStringInfo(&command, ", nullstr %s",
										 quote_literal_cstr(null));
					}
					else if (strcmp(option->defname, "force_quote") == 0)
					{
						if (option->arg && IsA(option->arg, A_Star))
						{
							appendStringInfoString(&command, ", force_quote *");
						}
						else if (option->arg && IsA(option->arg, List))
						{
							appendStringInfoString(&command, ", force_quote (");

							List	   *columnNameList = castNode(List, option->arg);;
							ListCell   *columnNameCell = NULL;
							int			columnIndex = 0;

							foreach(columnNameCell, columnNameList)
							{
								char	   *columnName = strVal(lfirst(columnNameCell));

								/* add comma after first column */
								appendStringInfo(&command, "%s%s",
												 columnIndex > 0 ? ", " : "",
												 quote_identifier(columnName));

								columnIndex++;
							}

							appendStringInfoString(&command, ")");
						}
					}
				}

				break;
			}

		default:
			elog(ERROR, "unexpected format: %s", formatName);
	}

	/* end WITH options */
	appendStringInfoString(&command, ")");

	return ExecuteCopyToCommandOnPGDuckConnection(command.data,
												  leafFields,
												  schema,
												  destinationPath,
												  destinationFormat);
}


/*
 * TupleDescToProjectionList converts a PostgreSQL tuple descriptor to
 * projection list in string form that can be used for writes.
 */
static char *
TupleDescToProjectionListForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat)
{
	Assert(tupleDesc != NULL);

	StringInfoData projection;

	initStringInfo(&projection);

	bool		hasColumns = false;

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;

		if (hasColumns)
			appendStringInfoString(&projection, ", ");

		/*
		 * TimeTZ is stored as TIME (UTC-normalized) in Iceberg. We convert to
		 * UTC in PGDuckSerialize, so DuckDB should parse as TIME.
		 */
		if (columnTypeId == TIMETZOID && destinationFormat == DATA_FORMAT_ICEBERG)
			appendStringInfo(&projection, "CAST(%s AS TIME) AS ",
							 quote_identifier(columnName));

		/*
		 * In case of geometry, we write WKT in csv_writer.c and parse it as
		 * GEOMETRY via read_csv. Just before writing to the destination, we
		 * convert to a form that makes sense for the destination format,
		 * namely WKB blob in Parquet and GeoJSON in JSON.
		 *
		 * In case of CSV we preserve the WKT as written by csv_writer.c
		 */
		if (IsGeometryTypeId(columnTypeId))
		{
			if (destinationFormat == DATA_FORMAT_PARQUET ||
				destinationFormat == DATA_FORMAT_ICEBERG)
				appendStringInfo(&projection, "ST_AsWKB(%s) AS ",
								 quote_identifier(columnName));

			else if (destinationFormat == DATA_FORMAT_JSON)
				appendStringInfo(&projection, "ST_AsGeoJSON(%s) AS ",
								 quote_identifier(columnName));
		}
		appendStringInfo(&projection, "%s",
						 quote_identifier(columnName));

		hasColumns = true;
	}

	if (!hasColumns)
		/* no columns, fall back to SELECT * */
		return "*";

	return projection.data;
}


/*
 * TupleDescToColumnMapForWrite converts a PostgreSQL tuple descriptor to
 * a DuckDB columns map in string form.
 */
char *
TupleDescToColumnMapForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat)
{
	StringInfoData map;

	initStringInfo(&map);

	bool		hasColumns = false;

	appendStringInfoString(&map, "{");

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;
		int			columnTypeMod = column->atttypmod;
		DuckDBTypeInfo duckdbType = ChooseDuckDBEngineTypeForWrite(
																   MakePGType(columnTypeId, columnTypeMod),
																   destinationFormat);

		appendStringInfo(&map, "%s%s:%s",
						 hasColumns ? "," : "",
						 quote_literal_cstr(columnName),
						 quote_literal_cstr(duckdbType.typeName));

		hasColumns = true;
	}

	appendStringInfoString(&map, "}");

	return map.data;
}


/*
 * AppendFields appends comma-separated mappings from
 * a field name to a field ID to a DuckDB map in string form.
 */
void
AppendFields(StringInfo map, DataFileSchema * schema)
{
	bool		addComma = false;

	for (size_t fieldIdx = 0; fieldIdx < schema->nfields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];
		const char *fieldName = field->name;

		appendStringInfo(map, "%s%s: ",
						 addComma ? ", " : "",
						 quote_literal_cstr(fieldName));

		AppendFieldIdValue(map, field->type, field->id);

		addComma = true;
	}
}


/*
 * AppendFieldIdValue appends a field ID to a DuckDB map in string form.
 * The field ID is either a number or another map containing the field
 * ID for the current field and the subfields.
 *
 * https://duckdb.org/docs/sql/statements/copy
 */
static void
AppendFieldIdValue(StringInfo fieldIdsStr, Field * field, int fieldId)
{
#define CURRENT_FIELD_ID "__duckdb_field_id"

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			appendStringInfo(fieldIdsStr, "%d", fieldId);
			break;

		case FIELD_TYPE_LIST:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			FieldList  *listField = &field->field.list;

			appendStringInfoString(fieldIdsStr, ", element: ");
			AppendFieldIdValue(fieldIdsStr, listField->element, listField->elementId);

			appendStringInfoString(fieldIdsStr, "}");

			break;

		case FIELD_TYPE_MAP:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			FieldMap   *mapField = &field->field.map;

			appendStringInfoString(fieldIdsStr, ", key: ");
			AppendFieldIdValue(fieldIdsStr, mapField->key, mapField->keyId);
			appendStringInfoString(fieldIdsStr, ", value: ");
			AppendFieldIdValue(fieldIdsStr, mapField->value, mapField->valueId);

			appendStringInfoString(fieldIdsStr, "}");

			break;

		case FIELD_TYPE_STRUCT:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			DataFileSchema *structField = &field->field.structType;

			appendStringInfoString(fieldIdsStr, ", ");
			AppendFields(fieldIdsStr, structField);

			appendStringInfoString(fieldIdsStr, "}");
			break;
	}
}


/*
 * ParquetVersionToString converts a ParquetVersion to a string.
 */
static const char *
ParquetVersionToString(ParquetVersion version)
{
	switch (version)
	{
		case PARQUET_VERSION_V1:
			return "V1";

		case PARQUET_VERSION_V2:
			return "V2";

		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unexpected parquet version: %d", version)));
	}

	return NULL;
}


/*
 * ChooseDuckDBEngineTypeForWrite obtains a DuckDB type name for a given postgres
 * type, and codifies some of our limitations around arrays and decimals.
 *
 * NOTE: This function should stay in sync with ShouldUseDuckSerialization where
 * we decide how to write the values to the intermediate CSV. Here we decide how
 * DuckDB should parse those values. In particular, the format emitted by csv_writer.c
 * should be parseable by read_csv() when using the type decided by this function.
 */
static DuckDBTypeInfo
ChooseDuckDBEngineTypeForWrite(PGType postgresType,
							   CopyDataFormat destinationFormat)
{
	/*
	 * We prefer to treat all fields as text when writing CSV to preserve
	 * PostgreSQL serialization format.
	 */
	if (destinationFormat == DATA_FORMAT_CSV)
		return VARCHAR_TYPE;

	int32		postgresTypeMod = postgresType.postgresTypeMod;
	Oid			elementTypeId = get_element_type(postgresType.postgresTypeOid);
	bool		isArrayType = OidIsValid(elementTypeId);
	char	   *typeModifier = "";

	/*
	 * We can handle an array by treating the element type like the type that
	 * was passed in from here on out an add [] to the type name in the end.
	 */
	if (isArrayType)
		postgresType.postgresTypeOid = elementTypeId;

	DuckDBType	duckTypeId = GetDuckDBTypeForPGType(postgresType);

	if (duckTypeId == DUCKDB_TYPE_INVALID)
	{
		/*
		 * Treat any type that does not have a DuckDB equivalent as text.
		 */
		duckTypeId = DUCKDB_TYPE_VARCHAR;
	}
	else if (duckTypeId == DUCKDB_TYPE_DECIMAL)
	{
		/*
		 * PostgreSQL supports up to 1000 digits in numeric fields, while
		 * DuckDB supports up to 38.
		 *
		 * To make sure we do not break the limit, emit large numeric as text.
		 * Other systems might not understand that as numeric, but PostgreSQL
		 * can still parse it.
		 *
		 * https://duckdb.org/docs/sql/data_types/overview
		 * https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
		 */
		int			precision = -1;
		int			scale = -1;

		GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(postgresTypeMod, &precision, &scale);

		if (CanPushdownNumericToDuckdb(precision, scale) &&
			destinationFormat != DATA_FORMAT_ICEBERG)
		{
			/*
			 * happy case: we can map to DECIMAL(precision, scale)
			 */
			typeModifier = psprintf("(%d,%d)", precision, scale);
		}
		else
		{
			/*
			 * Read numeric as VARCHAR.  For Iceberg, the numeric validation
			 * wrapper will validate NaN/Inf and digit limits before casting
			 * to DECIMAL (for both scalars and arrays). For other cases, the
			 * precision is too big for DuckDB's DECIMAL type.
			 */
			duckTypeId = DUCKDB_TYPE_VARCHAR;
		}
	}
	else if (duckTypeId == DUCKDB_TYPE_TIME_TZ && destinationFormat == DATA_FORMAT_ICEBERG)
	{
		/*
		 * Iceberg only has a "time" type (no timezone). We convert timetz
		 * values to UTC in PGDuckSerialize, so DuckDB should parse as TIME.
		 */
		duckTypeId = DUCKDB_TYPE_TIME;
	}
	else if (duckTypeId == DUCKDB_TYPE_BLOB && destinationFormat == DATA_FORMAT_JSON)
	{
		/*
		 * We map bytea to text in JSON, because DuckDB's bytea text format is
		 * subtly different from PostgreSQL. It needs a separate \x for every
		 * 2 hex characters, otherwise it interprets the characters as ASCII
		 * bytes, so something like \xabab would be interpreted differently
		 * between PG and DuckDB
		 *
		 * This corresponds to ShouldUseDuckSerialization in csv_writer.c
		 */
		duckTypeId = DUCKDB_TYPE_VARCHAR;
		isArrayType = false;
	}
	else if (duckTypeId == DUCKDB_TYPE_INTERVAL && destinationFormat == DATA_FORMAT_ICEBERG)
	{
		/*
		 * Iceberg does not have a native interval type. We store intervals as
		 * struct(months BIGINT, days BIGINT, microseconds BIGINT) in both the
		 * Iceberg metadata and Parquet data files. For plain Parquet files,
		 * DuckDB uses its native INTERVAL type.
		 */
		char	   *intervalTypeName =
			psprintf("STRUCT(months BIGINT, days BIGINT, microseconds BIGINT)%s",
					 isArrayType ? "[]" : "");
		DuckDBTypeInfo typeInfo = {
			.typeId = DUCKDB_TYPE_STRUCT,
			.typeName = intervalTypeName,
			.isArrayType = isArrayType,
		};

		return typeInfo;
	}

	/*
	 * In case of both JSON and Parquet, composites/arrays/maps are serialized
	 * in a DuckDB- compatible format by csv_writer.c and parsed into the
	 * native DuckDB struct/list/map types by read_csv(). When writing to JSON
	 * they become JSON objects/array, and when writing to Parquet they are
	 * converted to native Parquet structures. That behaviour seems desirable
	 * for us as well, so we do not do any special processing other than
	 * emitting the appropriate type name/definition.
	 */

	char	   *typeName;

	if (duckTypeId == DUCKDB_TYPE_STRUCT || duckTypeId == DUCKDB_TYPE_MAP)
	{
		/* generate field names for struct/map */
		const char *structDef =
			GetFullDuckDBTypeNameForPGType(postgresType, destinationFormat);

		typeName = psprintf("%s%s", structDef, isArrayType ? "[]" : "");
	}
	else
		typeName = psprintf("%s%s%s",
							GetDuckDBTypeName(duckTypeId),
							typeModifier,
							isArrayType ? "[]" : "");

	DuckDBTypeInfo typeInfo = {
		.typeId = duckTypeId,
		.typeName = typeName,
		.isArrayType = isArrayType
	};

	return typeInfo;
}


/*
 * IsTemporalType returns true if the given type OID is a date,
 * timestamp, or timestamptz type.
 */
static bool
IsTemporalType(Oid typeOid)
{
	return typeOid == DATEOID ||
		typeOid == TIMESTAMPOID ||
		typeOid == TIMESTAMPTZOID;
}


/*
 * TypeContainsTemporal recursively checks whether a PostgreSQL type
 * contains any date/timestamp/timestamptz fields, including fields
 * nested inside structs, maps, and arrays.
 */
static bool
TypeContainsTemporal(Oid typeOid)
{
	if (IsTemporalType(typeOid))
		return true;

	/* array: check element type */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeContainsTemporal(elemType);

	/* map: check key and value types */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valueType = GetMapValueType(typeOid);

		return TypeContainsTemporal(keyType.postgresTypeOid) ||
			TypeContainsTemporal(valueType.postgresTypeOid);
	}

	/* composite/struct: check each field */
	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;

			if (TypeContainsTemporal(att->atttypid))
			{
				ReleaseTupleDesc(tupdesc);
				return true;
			}
		}

		ReleaseTupleDesc(tupdesc);
	}

	return false;
}


/*
 * AppendTemporalRangeCheck appends a DuckDB boolean expression that
 * checks whether a single scalar temporal expression is out of range
 * or +-infinity.
 *
 * Returns true if the expression is out of range, which callers use
 * to trigger error() or clamping.  Does NOT include a NULL check —
 * callers handle NULL propagation.
 *
 * The isfinite() check comes first so that year() is never evaluated
 * on an infinite value, which would be undefined in DuckDB.
 */
static void
AppendTemporalRangeCheck(StringInfo buf, const char *expr, Oid typeOid)
{
	Assert(IsTemporalType(typeOid));

	if (typeOid == DATEOID)
	{
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s) < -4712 OR year(%s) > 9999)",
						 expr, expr, expr);
	}
	else if (typeOid == TIMESTAMPTZOID)
	{
		/*
		 * DuckDB's ICU-based year() for TIMESTAMPTZ returns the era-based
		 * year (always positive, e.g. 1 for 1 BC) rather than the ISO year (0
		 * for 1 BC).  Cast to TIMESTAMP first so the core year() function is
		 * used, which returns the correct astronomical year.
		 */
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s::TIMESTAMP) < 1 OR year(%s::TIMESTAMP) > 9999)",
						 expr, expr, expr);
	}
	else
	{
		/* TIMESTAMPOID */
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s) < 1 OR year(%s) > 9999)",
						 expr, expr, expr);
	}
}


/*
 * AppendTemporalLowerBoundCheck appends a DuckDB boolean expression that
 * checks whether a scalar temporal expression is below the lower bound
 * of Iceberg's supported range.
 *
 * Uses a direct comparison against the min literal, which correctly
 * handles +-infinity: -infinity < min is true → clamp to min,
 * +infinity < min is false → clamp to max.
 */
static void
AppendTemporalLowerBoundCheck(StringInfo buf, const char *expr, Oid typeOid)
{
	Assert(IsTemporalType(typeOid));

	appendStringInfo(buf, "%s < %s", expr, GetTemporalMinLiteral(typeOid));
}


/*
 * GetTemporalMinLiteral returns the DuckDB literal for the minimum
 * supported temporal value in Iceberg for the given type.
 */
static const char *
GetTemporalMinLiteral(Oid typeOid)
{
	if (typeOid == DATEOID)
		return "DATE '-4712-01-01'";
	if (typeOid == TIMESTAMPTZOID)
		return "TIMESTAMPTZ '0001-01-01 00:00:00+00'";
	return "TIMESTAMP '0001-01-01 00:00:00'";
}


/*
 * GetTemporalMaxLiteral returns the DuckDB literal for the maximum
 * supported temporal value in Iceberg for the given type.
 */
static const char *
GetTemporalMaxLiteral(Oid typeOid)
{
	if (typeOid == DATEOID)
		return "DATE '9999-12-31'";
	if (typeOid == TIMESTAMPTZOID)
		return "TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'";
	return "TIMESTAMP '9999-12-31 23:59:59.999999'";
}


/*
 * AppendTemporalOutOfRangeAction appends the THEN clause for an
 * out-of-range or infinite temporal value.
 *
 * In error mode, emits: error('...')
 * In clamp mode, emits: CASE WHEN (lower_check) THEN min ELSE max END
 */
static void
AppendTemporalOutOfRangeAction(StringInfo buf, const char *expr, Oid typeOid)
{
	if (IcebergOutOfRangeValues == ICEBERG_OUT_OF_RANGE_CLAMP)
	{
		appendStringInfoString(buf, "CASE WHEN ");
		AppendTemporalLowerBoundCheck(buf, expr, typeOid);
		appendStringInfo(buf, " THEN %s ELSE %s END",
						 GetTemporalMinLiteral(typeOid),
						 GetTemporalMaxLiteral(typeOid));
	}
	else
	{
		const char *errMsg = (typeOid == DATEOID) ?
			"date out of range for Iceberg" :
			"timestamp out of range for Iceberg";

		appendStringInfo(buf, "error('%s')", errMsg);
	}
}


/*
 * AppendTemporalValidationExpr appends a DuckDB CASE expression that
 * validates a scalar temporal value against Iceberg's supported range.
 *
 * In error mode (default), out-of-range values cause DuckDB's error()
 * to throw, which propagates back to PostgreSQL as an ereport(ERROR).
 *
 * In clamp mode, out-of-range values are clamped to the nearest
 * valid boundary value.
 */
static void
AppendTemporalValidationExpr(StringInfo buf, const char *expr,
							 const char *alias, Oid typeOid)
{
	appendStringInfo(buf, "CASE WHEN %s IS NOT NULL AND ", expr);
	AppendTemporalRangeCheck(buf, expr, typeOid);
	appendStringInfoString(buf, " THEN ");
	AppendTemporalOutOfRangeAction(buf, expr, typeOid);
	appendStringInfo(buf, " ELSE %s END AS %s", expr, alias);
}


static void AppendValidatedColumnExpr(StringInfo buf, const char *expr,
									  Oid typeOid, int depth);


/*
 * AppendValidatedListExpr wraps array/list elements with temporal
 * validation using DuckDB's list_transform().
 *
 * The 'depth' parameter is used to generate unique lambda variable
 * names (__v0, __v1, ...) to avoid collisions in nested lambdas.
 */
static void
AppendValidatedListExpr(StringInfo buf, const char *expr,
						Oid elemTypeOid, int depth)
{
	char	   *lambdaVar = psprintf("__v%d", depth);
	StringInfoData lambdaBody;

	initStringInfo(&lambdaBody);
	AppendValidatedColumnExpr(&lambdaBody, lambdaVar, elemTypeOid, depth + 1);

	appendStringInfo(buf, "list_transform(%s, %s -> %s)",
					 expr, lambdaVar, lambdaBody.data);

	pfree(lambdaBody.data);
	pfree(lambdaVar);
}


/*
 * AppendValidatedColumnExpr recursively appends a DuckDB expression
 * that validates all temporal fields within a value of the given type.
 *
 * Handles:
 *   - Scalar temporal types (date, timestamp, timestamptz)
 *   - Arrays of any type containing temporal fields
 *   - Structs with temporal fields at any nesting depth
 *   - Maps whose values contain temporal fields
 *
 * In error mode, raises an error (via DuckDB's error() function) for
 * out-of-range temporal values.  In clamp mode, clamps them to the
 * nearest valid boundary value.
 */
static void
AppendValidatedColumnExpr(StringInfo buf, const char *expr,
						  Oid typeOid, int depth)
{
	if (IsTemporalType(typeOid))
	{
		appendStringInfo(buf, "CASE WHEN %s IS NOT NULL AND ", expr);
		AppendTemporalRangeCheck(buf, expr, typeOid);
		appendStringInfoString(buf, " THEN ");
		AppendTemporalOutOfRangeAction(buf, expr, typeOid);
		appendStringInfo(buf, " ELSE %s END", expr);
		return;
	}

	/* array: validate each element */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType) && TypeContainsTemporal(elemType))
	{
		AppendValidatedListExpr(buf, expr, elemType, depth);
		return;
	}

	/* map: validate keys and/or values via map_entries + list_transform */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		bool		keyTemporal = TypeContainsTemporal(keyType.postgresTypeOid);
		bool		valTemporal = TypeContainsTemporal(valType.postgresTypeOid);

		if (keyTemporal || valTemporal)
		{
			char	   *entryVar = psprintf("__v%d", depth);
			StringInfoData keyBody;
			StringInfoData valBody;

			initStringInfo(&keyBody);
			initStringInfo(&valBody);

			if (keyTemporal)
				AppendValidatedColumnExpr(&keyBody,
										  psprintf("%s.key", entryVar),
										  keyType.postgresTypeOid,
										  depth + 1);
			else
				appendStringInfo(&keyBody, "%s.key", entryVar);

			if (valTemporal)
				AppendValidatedColumnExpr(&valBody,
										  psprintf("%s.value", entryVar),
										  valType.postgresTypeOid,
										  depth + 1);
			else
				appendStringInfo(&valBody, "%s.value", entryVar);

			appendStringInfo(buf,
							 "map_from_entries(list_transform(map_entries(%s), "
							 "%s -> struct_pack(key := %s, value := %s)))",
							 expr, entryVar, keyBody.data, valBody.data);

			pfree(keyBody.data);
			pfree(valBody.data);
			pfree(entryVar);
			return;
		}
	}

	/* composite/struct: rebuild with validated fields */
	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		hasTemporalField = false;

		/* first check if any field needs validation */
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;

			if (TypeContainsTemporal(att->atttypid))
			{
				hasTemporalField = true;
				break;
			}
		}

		if (hasTemporalField)
		{
			/*
			 * Rebuild the struct with struct_pack, validating temporal
			 * fields.
			 */
			appendStringInfoString(buf, "struct_pack(");

			bool		first = true;

			for (int i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);

				if (att->attisdropped)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				const char *fieldName = NameStr(att->attname);
				char	   *fieldExpr = psprintf("%s.%s", expr,
												 quote_identifier(fieldName));

				appendStringInfo(buf, "%s := ", quote_identifier(fieldName));

				if (TypeContainsTemporal(att->atttypid))
					AppendValidatedColumnExpr(buf, fieldExpr, att->atttypid,
											  depth);
				else
					appendStringInfoString(buf, fieldExpr);

				pfree(fieldExpr);
			}

			appendStringInfoChar(buf, ')');

			ReleaseTupleDesc(tupdesc);
			return;
		}

		ReleaseTupleDesc(tupdesc);
	}

	/* no temporal content, pass through */
	appendStringInfoString(buf, expr);
}


/*
 * WrapQueryWithIcebergTemporalValidation wraps a DuckDB query with
 * range-checking expressions for date/timestamp/timestamptz columns
 * when writing to Iceberg format.
 *
 * All writes (direct INSERT, INSERT..SELECT, COPY FROM) flow through
 * WriteQueryResultTo, which calls this wrapper.  It ensures that
 * out-of-range temporal values are caught at the DuckDB level before
 * being written to Parquet data files.
 *
 * Handles temporal types at any nesting depth: top-level scalars,
 * arrays, structs, and maps.
 *
 * If no temporal columns are found, the original query is returned as-is.
 */
static char *
WrapQueryWithIcebergTemporalValidation(char *query, TupleDesc tupleDesc)
{
	if (tupleDesc == NULL)
		return query;

	/* quick check: any temporal columns at all? */
	bool		hasTemporalCol = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeContainsTemporal(attr->atttypid))
		{
			hasTemporalCol = true;
			break;
		}
	}

	if (!hasTemporalCol)
		return query;

	StringInfoData wrapped;

	initStringInfo(&wrapped);
	appendStringInfoString(&wrapped, "SELECT ");

	bool		first = true;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(&wrapped, ", ");
		first = false;

		const char *colName = quote_identifier(NameStr(attr->attname));
		Oid			typeId = attr->atttypid;

		if (TypeContainsTemporal(typeId))
		{
			if (IsTemporalType(typeId))
			{
				/* scalar date/timestamp/timestamptz */
				AppendTemporalValidationExpr(&wrapped, colName, colName,
											 typeId);
			}
			else
			{
				/*
				 * Complex type containing temporal fields — generate a
				 * recursive validation expression.
				 */
				AppendValidatedColumnExpr(&wrapped, colName, typeId, 0);
				appendStringInfo(&wrapped, " AS %s", colName);
			}
		}
		else
		{
			appendStringInfoString(&wrapped, colName);
		}
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_temporal_check", query);

	return wrapped.data;
}


/*
 * GetNumericMaxLiteral returns the maximum positive DECIMAL(p,s) literal.
 *
 * For example, DECIMAL(38,9) → "99999999999999999999999999999.999999999"
 */
static const char *
GetNumericMaxLiteral(int precision, int scale)
{
	StringInfoData result;
	int			integralDigits = precision - scale;

	initStringInfo(&result);

	for (int i = 0; i < integralDigits; i++)
		appendStringInfoChar(&result, '9');

	if (scale > 0)
	{
		appendStringInfoChar(&result, '.');
		for (int i = 0; i < scale; i++)
			appendStringInfoChar(&result, '9');
	}

	return result.data;
}


/*
 * AppendNumericNaNAction appends the THEN clause for a NaN value.
 *
 * In error mode (default), emits: error('...')
 * In clamp mode, emits: NULL::DECIMAL(p,s) (or NULL::VARCHAR)
 *
 * NaN has no numeric equivalent, so we clamp to NULL.
 */
static void
AppendNumericNaNAction(StringInfo buf, const char *expr, int precision,
					   int scale)
{
	if (IcebergOutOfRangeValues == ICEBERG_OUT_OF_RANGE_CLAMP)
	{
		if (CanPushdownNumericToDuckdb(precision, scale))
			appendStringInfo(buf, "NULL::DECIMAL(%d,%d)", precision, scale);
		else
			appendStringInfoString(buf, "NULL::VARCHAR");
	}
	else
	{
		appendStringInfo(buf,
						 "error(CONCAT('NaN is not allowed for numeric type: ', "
						 "CAST(%s AS VARCHAR)))",
						 expr);
	}
}


/*
 * AppendNumericOutOfRangeAction appends the THEN clause for an
 * out-of-range numeric value (Infinity or digit overflow).
 *
 * In error mode (default), emits: error('...')
 * In clamp mode, emits a sign-aware expression that returns the
 * maximum or minimum DECIMAL(p,s) literal (negative max for negative
 * values, positive max for positive values).
 */
static void
AppendNumericOutOfRangeAction(StringInfo buf, const char *expr,
							  int precision, int scale,
							  const char *errMsg)
{
	if (IcebergOutOfRangeValues == ICEBERG_OUT_OF_RANGE_CLAMP)
	{
		const char *maxLit = GetNumericMaxLiteral(precision, scale);

		if (CanPushdownNumericToDuckdb(precision, scale))
		{
			appendStringInfo(buf,
							 "CASE WHEN STARTS_WITH(LTRIM(CAST(%s AS VARCHAR)), '-') "
							 "THEN CAST('-%s' AS DECIMAL(%d,%d)) "
							 "ELSE CAST('%s' AS DECIMAL(%d,%d)) END",
							 expr, maxLit, precision, scale,
							 maxLit, precision, scale);
		}
		else
		{
			appendStringInfo(buf,
							 "CASE WHEN STARTS_WITH(LTRIM(CAST(%s AS VARCHAR)), '-') "
							 "THEN '-%s' ELSE '%s' END",
							 expr, maxLit, maxLit);
		}
	}
	else
	{
		appendStringInfo(buf,
						 "error(CONCAT('%s: ', CAST(%s AS VARCHAR)))",
						 errMsg, expr);
	}
}


/*
 * AppendNumericValidationCaseExpr appends a DuckDB CASE expression
 * that validates a single numeric value for Iceberg compatibility.
 *
 * The generated expression can be used standalone or inside a
 * list_transform lambda for array elements.
 *
 * Checks:
 *  - NaN → error or NULL (NaN has no numeric equivalent)
 *  - Infinity → error or clamp to min/max DECIMAL
 *  - For unbounded numeric: integral and decimal digit limits →
 *    error or clamp to min/max DECIMAL
 *  - Otherwise: CAST to DECIMAL(precision, scale) (or keep VARCHAR if
 *    the precision is too large for DuckDB)
 *
 * In error mode (default), invalid values raise error().
 * In clamp mode, NaN becomes NULL and out-of-range values are clamped
 * to the min/max DECIMAL(p,s) based on sign.
 *
 * Does NOT append an alias — callers add "AS alias" as needed.
 */
static void
AppendNumericValidationCaseExpr(StringInfo buf, const char *expr, int typmod)
{
	int			precision;
	int			scale;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &precision, &scale);

	bool		isUnbounded = IsUnboundedNumeric(NUMERICOID, typmod);

	/* check for NaN */
	appendStringInfo(buf,
					 "CASE WHEN LOWER(TRIM(CAST(%s AS VARCHAR))) = 'nan' THEN ",
					 expr);
	AppendNumericNaNAction(buf, expr, precision, scale);

	/* check for Infinity */
	appendStringInfo(buf,
					 " WHEN LOWER(TRIM(CAST(%s AS VARCHAR))) "
					 "IN ('infinity', '+infinity', '-infinity', "
					 "'inf', '+inf', '-inf') THEN ",
					 expr);
	AppendNumericOutOfRangeAction(buf, expr, precision, scale,
								  "Infinity values are not allowed for "
								  "numeric type");

	if (isUnbounded)
	{
		int			maxIntegralDigits = precision - scale;

		/* check integral digit count */
		appendStringInfo(buf,
						 " WHEN %s IS NOT NULL AND "
						 "LENGTH(REGEXP_REPLACE(SPLIT_PART("
						 "LTRIM(CAST(%s AS VARCHAR), '+-'), '.', 1), "
						 "'[^0-9]', '', 'g')) > %d THEN ",
						 expr, expr, maxIntegralDigits);
		AppendNumericOutOfRangeAction(buf, expr, precision, scale,
									  psprintf("unbounded numeric type exceeds max "
											   "allowed digits %d before decimal point",
											   maxIntegralDigits));

		/*
		 * We intentionally do NOT check the decimal digit count here. When
		 * the PostgreSQL column is unbounded numeric, values may have more
		 * fractional digits than the Iceberg scale (e.g. random() produces
		 * 15+ digits).  The CAST to DECIMAL(p,s) in the ELSE branch will
		 * round excess fractional digits, which is correct.
		 */
	}

	if (CanPushdownNumericToDuckdb(precision, scale))
	{
		if (!isUnbounded)
		{
			/*
			 * For bounded numerics, add explicit range checks so that
			 * out-of-range values are caught before the final CAST. In clamp
			 * mode this clamps to min/max; in error mode it raises a clear
			 * error instead of a raw DuckDB conversion error.
			 *
			 * TRY_CAST to DECIMAL(38, target_scale) for safe comparison. This
			 * gives up to (38 - scale) integer digits — always enough —
			 * while preserving exact fractional precision.
			 */
			const char *maxLit = GetNumericMaxLiteral(precision, scale);

			appendStringInfo(buf,
							 " WHEN %s IS NOT NULL AND "
							 "TRY_CAST(%s AS DECIMAL(38,%d)) > "
							 "CAST('%s' AS DECIMAL(38,%d)) THEN ",
							 expr,
							 expr, scale,
							 maxLit, scale);
			AppendNumericOutOfRangeAction(buf, expr, precision, scale,
										  psprintf("numeric value exceeds max "
												   "allowed value for DECIMAL(%d,%d)",
												   precision, scale));

			appendStringInfo(buf,
							 " WHEN %s IS NOT NULL AND "
							 "TRY_CAST(%s AS DECIMAL(38,%d)) < "
							 "CAST('-%s' AS DECIMAL(38,%d)) THEN ",
							 expr,
							 expr, scale,
							 maxLit, scale);
			AppendNumericOutOfRangeAction(buf, expr, precision, scale,
										  psprintf("numeric value exceeds min "
												   "allowed value for DECIMAL(%d,%d)",
												   precision, scale));
		}

		appendStringInfo(buf,
						 " ELSE CAST(%s AS DECIMAL(%d,%d)) END",
						 expr, precision, scale);
	}
	else
	{
		/* precision too big for DuckDB DECIMAL, keep as VARCHAR */
		appendStringInfo(buf,
						 " ELSE %s END",
						 expr);
	}
}


/*
 * AppendNumericValidationExpr appends a CASE expression for a scalar
 * numeric column, including the "AS alias" suffix.
 */
static void
AppendNumericValidationExpr(StringInfo buf, const char *expr,
							const char *alias, int typmod)
{
	AppendNumericValidationCaseExpr(buf, expr, typmod);
	appendStringInfo(buf, " AS %s", alias);
}


/*
 * AppendNumericArrayValidationExpr validates each element of a
 * numeric[] column using DuckDB's list_transform().
 *
 * Generates: list_transform(col, __nv -> CASE ... END) AS col
 */
static void
AppendNumericArrayValidationExpr(StringInfo buf, const char *expr,
								 const char *alias, int typmod)
{
	appendStringInfo(buf, "list_transform(%s, __nv -> ", expr);
	AppendNumericValidationCaseExpr(buf, "__nv", typmod);
	appendStringInfo(buf, ") AS %s", alias);
}


/*
 * IsNumericArrayType returns true if the given type OID is an array
 * whose element type is NUMERICOID.
 */
static bool
IsNumericArrayType(Oid typeOid)
{
	Oid			elemType = get_element_type(typeOid);

	return OidIsValid(elemType) && elemType == NUMERICOID;
}


/*
 * NeutralizeNumericCastsInQuery replaces ::numeric(p,s) casts in the
 * query string with ::text.
 *
 * For INSERT..SELECT pushdown, PostgreSQL's planner adds type coercions
 * like ::numeric(3,0) to match the target column type.  When DuckDB
 * executes this cast, it may fail for values that exceed the target
 * precision (e.g. a large value being cast to DECIMAL(3,0)).
 *
 * By replacing these casts with ::text, the value passes through as a
 * VARCHAR string, and the numeric validation wrapper handles range
 * checking, clamping, and final casting to the target DECIMAL type.
 */
static char *
NeutralizeNumericCastsInQuery(char *query)
{
	StringInfoData result;
	const char *p = query;

	initStringInfo(&result);

	while (*p != '\0')
	{
		/*
		 * Look for the pattern ::numeric( followed by digits, comma, optional
		 * space, digits, and closing paren.
		 */
		if (strncmp(p, "::numeric(", 10) == 0)
		{
			const char *start = p;
			const char *scan = p + 10;	/* skip "::numeric(" */
			bool		valid = true;

			/* expect one or more digits */
			if (!isdigit((unsigned char) *scan))
				valid = false;
			while (isdigit((unsigned char) *scan))
				scan++;

			/* expect comma and optional space */
			if (valid && *scan == ',')
				scan++;
			else
				valid = false;
			while (*scan == ' ')
				scan++;

			/* expect one or more digits */
			if (valid && !isdigit((unsigned char) *scan))
				valid = false;
			while (isdigit((unsigned char) *scan))
				scan++;

			/* expect closing paren */
			if (valid && *scan == ')')
			{
				scan++;			/* skip ')' */
				appendStringInfoString(&result, "::text");
				p = scan;
				continue;
			}

			/* not a match, copy the character as-is */
			(void) start;		/* suppress unused warning */
		}

		appendStringInfoChar(&result, *p);
		p++;
	}

	return result.data;
}


/*
 * WrapQueryWithIcebergNumericValidation wraps a DuckDB query with
 * validation expressions for numeric columns when writing to Iceberg.
 *
 * All writes (direct INSERT, INSERT..SELECT, COPY FROM) flow through
 * WriteQueryResultTo, which calls this wrapper.  It ensures that:
 *  - NaN/Infinity values are rejected or clamped
 *  - Unbounded numeric digit limits are enforced
 *  - Bounded numeric values exceeding DECIMAL(p,s) range are clamped
 *  - Numeric values are cast from VARCHAR to DECIMAL(p,s)
 *
 * Handles both scalar numeric and numeric[] columns.
 *
 * If no numeric columns are found, the original query is returned as-is.
 */
static char *
WrapQueryWithIcebergNumericValidation(char *query, TupleDesc tupleDesc)
{
	if (tupleDesc == NULL)
		return query;

	/* quick check: any numeric columns (scalar or array)? */
	bool		hasNumericCol = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (attr->atttypid == NUMERICOID || IsNumericArrayType(attr->atttypid))
		{
			hasNumericCol = true;
			break;
		}
	}

	if (!hasNumericCol)
		return query;

	/*
	 * Replace ::numeric(p,s) casts in the inner query with ::text. This
	 * prevents DuckDB from failing on bounded numeric casts (e.g.
	 * ::numeric(3,0)) when the source value exceeds the target precision. The
	 * validation wrapper handles proper range checking and casting below.
	 */
	query = NeutralizeNumericCastsInQuery(query);

	StringInfoData wrapped;

	initStringInfo(&wrapped);
	appendStringInfoString(&wrapped, "SELECT ");

	bool		first = true;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(&wrapped, ", ");
		first = false;

		const char *colName = quote_identifier(NameStr(attr->attname));

		if (attr->atttypid == NUMERICOID)
		{
			AppendNumericValidationExpr(&wrapped, colName, colName,
										attr->atttypmod);
		}
		else if (IsNumericArrayType(attr->atttypid))
		{
			AppendNumericArrayValidationExpr(&wrapped, colName, colName,
											 attr->atttypmod);
		}
		else
		{
			appendStringInfoString(&wrapped, colName);
		}
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_numeric_check", query);

	return wrapped.data;
}
