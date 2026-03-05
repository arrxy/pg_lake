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
 * Value validation for writes to Parquet / Iceberg data files.
 *
 * Validates PostgreSQL Datum values in-process before they are serialized
 * to the intermediate CSV that DuckDB converts to Parquet.  Depending on
 * the out_of_range_values table or COPY option, out-of-range values
 * either raise an error or are clamped to boundary values.
 *
 * Temporal (date, timestamp, timestamptz):
 *   - +-infinity and out-of-range years are rejected or clamped.
 *   - Date supports the proleptic Gregorian range -4712 .. 9999.
 *   - Timestamp/TimestampTZ are limited to 0001 .. 9999.
 *
 * Numeric:
 *   - NaN values are rejected or clamped to NULL.
 *   - +-Infinity values are rejected or clamped to min/max DECIMAL.
 *   - Values exceeding DECIMAL(p,s) integral digits are rejected or clamped.
 *   - Values exceeding DECIMAL(p,s) fractional digits are rejected or rounded.
 *
 * Handles nested types: arrays, structs, and maps.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "funcapi.h"
#include "pg_lake/pgduck/write_validation.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/util/rel_utils.h"
#include "foreign/foreign.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


/* ================================================================
 * Temporal boundary constants (Postgres DateADT / Timestamp values)
 *
 * Date: full proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 * Timestamp/TimestampTZ: common-era range 0001-01-01 .. 9999-12-31.
 * ================================================================ */
#define TEMPORAL_DATE_MIN_YEAR		(-4712)
#define TEMPORAL_TIMESTAMP_MIN_YEAR	1
#define TEMPORAL_MAX_YEAR			9999

static bool NeedsValidation(Oid typeOid, int32 typmod);
static bool IsTemporalType(Oid typeOid);
static int	GetYearFromDate(DateADT d);
static int	GetYearFromTimestamp(Timestamp ts);
static Datum ClampOrErrorTemporal(Datum value, Oid typeOid, int year,
								  OutOfRangePolicy policy);
static DateADT MakeDateFromYMD(int y, int m, int d);
static Timestamp MakeTimestampUsec(int y, int m, int d, int h, int min, int sec, int usec);
static Datum ValidateNumericDatum(Datum value, int32 typmod,
								  OutOfRangePolicy policy, bool *isNull);
static Datum ValidateArrayDatum(Datum value, Oid elemTypeOid, int32 elemTypmod,
								OutOfRangePolicy policy);
static Datum ValidateCompositeDatum(Datum value, Oid typeOid,
									OutOfRangePolicy policy);
static Datum ValidateMapDatum(Datum value, Oid mapTypeOid,
							  OutOfRangePolicy policy);


/*
 * GetOutOfRangePolicyFromOptions reads the "out_of_range_values" option
 * from a list of DefElem options (table options or COPY options).
 *
 * Returns OUT_OF_RANGE_ERROR if the option is set to "error",
 * OUT_OF_RANGE_CLAMP otherwise (including when not present).
 */
OutOfRangePolicy
GetOutOfRangePolicyFromOptions(List *options)
{
	char	   *value = GetStringOption(options, "out_of_range_values", false);

	if (value != NULL && strcmp(value, "error") == 0)
		return OUT_OF_RANGE_ERROR;

	return OUT_OF_RANGE_CLAMP;
}


/*
 * GetOutOfRangePolicyForTable reads the "out_of_range_values" table option
 * for the given relation.  Returns NONE for non-pg_lake / non-iceberg tables.
 */
OutOfRangePolicy
GetOutOfRangePolicyForTable(Oid relationId)
{
	if (!IsPgLakeForeignTableById(relationId) &&
		!IsPgLakeIcebergForeignTableById(relationId))
		return OUT_OF_RANGE_NONE;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return GetOutOfRangePolicyFromOptions(foreignTable->options);
}


/*
 * IsTemporalType returns true for date, timestamp, or timestamptz.
 */
static bool
IsTemporalType(Oid typeOid)
{
	return typeOid == DATEOID ||
		typeOid == TIMESTAMPOID ||
		typeOid == TIMESTAMPTZOID;
}


/*
 * NeedsValidation returns true if the given type (possibly nested)
 * contains any temporal or numeric fields that require validation.
 */
static bool
NeedsValidation(Oid typeOid, int32 typmod)
{
	if (IsTemporalType(typeOid))
		return true;

	if (typeOid == NUMERICOID)
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return NeedsValidation(elemType, typmod);

	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valueType = GetMapValueType(typeOid);

		return NeedsValidation(keyType.postgresTypeOid, keyType.postgresTypeMod) ||
			NeedsValidation(valueType.postgresTypeOid, valueType.postgresTypeMod);
	}

	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		result = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;

			if (NeedsValidation(att->atttypid, att->atttypmod))
			{
				result = true;
				break;
			}
		}

		ReleaseTupleDesc(tupdesc);
		return result;
	}

	return false;
}


/*
 * ValidateDatum validates a single Datum value for out-of-range
 * temporal or numeric issues before writing to Parquet.
 *
 * For scalar temporal/numeric types, validates directly.
 * For arrays, maps, and composites, recursively validates elements.
 *
 * *isNull may be set to true if the policy clamps NaN to NULL.
 * On entry, *isNull must reflect whether the Datum is null.
 */
Datum
ValidateDatum(Datum value, Oid typeOid, int32 typmod,
			  OutOfRangePolicy policy, bool *isNull)
{
	if (*isNull || policy == OUT_OF_RANGE_NONE)
		return value;

	if (IsTemporalType(typeOid))
		return ValidateTemporalDatum(value, typeOid, policy);

	if (typeOid == NUMERICOID)
		return ValidateNumericDatum(value, typmod, policy, isNull);

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType) && NeedsValidation(elemType, typmod))
		return ValidateArrayDatum(value, elemType, typmod, policy);

	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);

		if (NeedsValidation(keyType.postgresTypeOid, keyType.postgresTypeMod) ||
			NeedsValidation(valType.postgresTypeOid, valType.postgresTypeMod))
			return ValidateMapDatum(value, typeOid, policy);
	}

	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE && NeedsValidation(typeOid, typmod))
		return ValidateCompositeDatum(value, typeOid, policy);

	return value;
}


/*
 * TupleDescNeedsValidation returns true if any column in the tuple
 * descriptor has a type that requires out-of-range validation.
 */
bool
TupleDescNeedsValidation(TupleDesc tupleDesc, OutOfRangePolicy policy)
{
	if (tupleDesc == NULL || policy == OUT_OF_RANGE_NONE)
		return false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (NeedsValidation(attr->atttypid, attr->atttypmod))
			return true;
	}

	return false;
}


/* ================================================================
 * Temporal validation
 * ================================================================ */

/*
 * GetYearFromDate extracts the year from a PostgreSQL DateADT.
 */
static int
GetYearFromDate(DateADT d)
{
	int			y,
				m,
				day;

	j2date(d + POSTGRES_EPOCH_JDATE, &y, &m, &day);
	return y;
}


/*
 * GetYearFromTimestamp extracts the year from a PostgreSQL Timestamp
 * (works for both Timestamp and TimestampTz since they share the
 * same representation).
 */
static int
GetYearFromTimestamp(Timestamp ts)
{
	struct pg_tm tt;
	fsec_t		fsec;

	if (timestamp2tm(ts, NULL, &tt, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	return tt.tm_year;
}


/*
 * MakeDateFromYMD creates a DateADT from year, month, day.
 */
static DateADT
MakeDateFromYMD(int y, int m, int d)
{
	return date2j(y, m, d) - POSTGRES_EPOCH_JDATE;
}


/*
 * MakeTimestampUsec creates a Timestamp from date/time components
 * including microseconds (no timezone; for TimestampTz the caller casts).
 */
static Timestamp
MakeTimestampUsec(int y, int m, int d, int h, int min, int sec, int usec)
{
	DateADT		date = MakeDateFromYMD(y, m, d);
	Timestamp	result;

	result = (Timestamp) date * USECS_PER_DAY +
		((((h * 60) + min) * 60) + sec) * USECS_PER_SEC + usec;

	return result;
}


/*
 * ClampOrErrorTemporal handles an out-of-range temporal value.
 *
 * In error mode: raises an error.
 * In clamp mode: returns the nearest boundary value.
 */
static Datum
ClampOrErrorTemporal(Datum value, Oid typeOid, int year,
					 OutOfRangePolicy policy)
{
	if (policy == OUT_OF_RANGE_ERROR)
	{
		const char *errMsg = (typeOid == DATEOID) ?
			"date out of range" :
			"timestamp out of range";

		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("%s", errMsg)));
	}

	/*
	 * Clamp mode: determine if value is below or above range.
	 *
	 * For infinity values, NOBEGIN = -infinity → clamp to min, NOEND =
	 * +infinity → clamp to max.
	 *
	 * For finite values, use the extracted year to decide direction.
	 */
	bool		clampToMin;

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			clampToMin = DATE_IS_NOBEGIN(d);
		else
			clampToMin = (year < TEMPORAL_DATE_MIN_YEAR);

		if (clampToMin)
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_DATE_MIN_YEAR, 1, 1));
		else
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_MAX_YEAR, 12, 31));
	}
	else if (typeOid == TIMESTAMPOID)
	{
		Timestamp	ts = DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		if (clampToMin)
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_TIMESTAMP_MIN_YEAR, 1, 1, 0, 0, 0, 0));
		else
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_MAX_YEAR, 12, 31, 23, 59, 59, 999999));
	}
	else
	{
		/* TIMESTAMPTZOID */
		TimestampTz ts = DatumGetTimestampTz(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		if (clampToMin)
			return TimestampTzGetDatum(
									   MakeTimestampUsec(TEMPORAL_TIMESTAMP_MIN_YEAR, 1, 1, 0, 0, 0, 0));
		else
			return TimestampTzGetDatum(
									   MakeTimestampUsec(TEMPORAL_MAX_YEAR, 12, 31, 23, 59, 59, 999999));
	}
}


/*
 * ValidateTemporalDatum validates a date, timestamp, or timestamptz Datum.
 */
Datum
ValidateTemporalDatum(Datum value, Oid typeOid, OutOfRangePolicy policy)
{
	Assert(IsTemporalType(typeOid));

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			return ClampOrErrorTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromDate(d);

		if (year < TEMPORAL_DATE_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ClampOrErrorTemporal(value, typeOid, year, policy);
	}
	else
	{
		Timestamp	ts = (typeOid == TIMESTAMPTZOID) ?
			DatumGetTimestampTz(value) :
			DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			return ClampOrErrorTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromTimestamp(ts);

		if (year < TEMPORAL_TIMESTAMP_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ClampOrErrorTemporal(value, typeOid, year, policy);
	}

	return value;
}


/* ================================================================
 * Numeric validation
 * ================================================================ */

/*
 * ValidateNumericDatum validates a numeric Datum against the target
 * DECIMAL(precision, scale) derived from the typmod.
 *
 * Sets *isNull = true when NaN is clamped to NULL.
 */
static Datum
ValidateNumericDatum(Datum value, int32 typmod, OutOfRangePolicy policy,
					 bool *isNull)
{
	int			precision,
				scale;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &precision, &scale);

	if (!CanPushdownNumericToDuckdb(precision, scale))
		return value;

	Numeric		num = DatumGetNumeric(value);

	/* NaN check */
	if (numeric_is_nan(num))
	{
		if (policy == OUT_OF_RANGE_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("NaN is not allowed for numeric type")));

		*isNull = true;
		return (Datum) 0;
	}

	/* Infinity check */
	if (numeric_is_inf(num))
	{
		if (policy == OUT_OF_RANGE_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("Infinity values are not allowed for numeric type")));

		/*
		 * Clamp: construct min/max DECIMAL(p,s).  Use the numeric_in path to
		 * parse e.g. "9999999.999" or "-9999999.999".
		 */
		char	   *strVal = DatumGetCString(
											 DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
		bool		isNeg = (strVal[0] == '-');

		StringInfoData maxLit;

		initStringInfo(&maxLit);

		int			maxIntDigits = precision - scale;

		if (isNeg)
			appendStringInfoChar(&maxLit, '-');

		if (maxIntDigits == 0)
			appendStringInfoChar(&maxLit, '0');
		else
			for (int i = 0; i < maxIntDigits; i++)
				appendStringInfoChar(&maxLit, '9');

		if (scale > 0)
		{
			appendStringInfoChar(&maxLit, '.');
			for (int i = 0; i < scale; i++)
				appendStringInfoChar(&maxLit, '9');
		}

		Datum		clamped = DirectFunctionCall3(numeric_in,
												  CStringGetDatum(maxLit.data),
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(-1));

		pfree(maxLit.data);
		return clamped;
	}

	/*
	 * Check digit counts using the normalized string representation.
	 * numeric_normalize returns a string without trailing zeros.
	 */
	char	   *strVal = numeric_normalize(num);
	char	   *p = strVal;

	if (*p == '-' || *p == '+')
		p++;

	/* skip leading zeros */
	while (*p == '0' && *(p + 1) != '.' && *(p + 1) != '\0')
		p++;

	int			integralDigits = 0;
	int			fractionalDigits = 0;

	/*
	 * Count integral digits.  A lone "0" before '.' or at end-of-string
	 * represents zero, which has 0 significant integral digits.
	 */
	if (*p == '0' && (*(p + 1) == '.' || *(p + 1) == '\0'))
	{
		p++;
	}
	else
	{
		while (*p && *p != '.')
		{
			integralDigits++;
			p++;
		}
	}

	if (*p == '.')
	{
		p++;

		while (*p)
		{
			fractionalDigits++;
			p++;
		}
	}

	int			maxIntegral = precision - scale;

	/* integral digit overflow */
	if (integralDigits > maxIntegral)
	{
		if (policy == OUT_OF_RANGE_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("numeric value exceeds max allowed digits %d "
							"before decimal point: %s",
							maxIntegral,
							DatumGetCString(DirectFunctionCall1(numeric_out,
																NumericGetDatum(num))))));

		/* clamp to max/min DECIMAL */
		bool		isNeg = (strVal[0] == '-');
		StringInfoData maxLit;

		initStringInfo(&maxLit);

		if (isNeg)
			appendStringInfoChar(&maxLit, '-');

		if (maxIntegral == 0)
			appendStringInfoChar(&maxLit, '0');
		else
			for (int i = 0; i < maxIntegral; i++)
				appendStringInfoChar(&maxLit, '9');

		if (scale > 0)
		{
			appendStringInfoChar(&maxLit, '.');
			for (int i = 0; i < scale; i++)
				appendStringInfoChar(&maxLit, '9');
		}

		Datum		clamped = DirectFunctionCall3(numeric_in,
												  CStringGetDatum(maxLit.data),
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(-1));

		pfree(maxLit.data);
		pfree(strVal);
		return clamped;
	}

	/*
	 * fractional digit overflow: error or let PG's numeric typmod coercion
	 * round
	 */
	if (fractionalDigits > scale)
	{
		if (policy == OUT_OF_RANGE_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("numeric value exceeds max allowed digits %d "
							"after decimal point: %s",
							scale,
							DatumGetCString(DirectFunctionCall1(numeric_out,
																NumericGetDatum(num))))));

		/*
		 * Clamp: round to the target scale by converting through text and
		 * re-parsing with the DuckDB-adjusted typmod, which triggers PG's
		 * built-in rounding for numeric.
		 */
		int32		adjustedTypmod = make_numeric_typmod(precision, scale);
		char	   *numStr = DatumGetCString(
											 DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
		Datum		rounded = DirectFunctionCall3(numeric_in,
												  CStringGetDatum(numStr),
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(adjustedTypmod));

		pfree(strVal);
		return rounded;
	}

	pfree(strVal);
	return value;
}


/* ================================================================
 * Array validation
 * ================================================================ */

/*
 * ValidateArrayDatum validates each element of a PostgreSQL array Datum.
 */
static Datum
ValidateArrayDatum(Datum value, Oid elemTypeOid, int32 elemTypmod,
				   OutOfRangePolicy policy)
{
	ArrayType  *arr = DatumGetArrayTypeP(value);
	int			nelems;
	Datum	   *elems;
	bool	   *nulls;
	int16		elemLen;
	bool		elemByVal;
	char		elemAlign;

	get_typlenbyvalalign(elemTypeOid, &elemLen, &elemByVal, &elemAlign);
	deconstruct_array(arr, elemTypeOid, elemLen, elemByVal, elemAlign,
					  &elems, &nulls, &nelems);

	bool		modified = false;

	for (int i = 0; i < nelems; i++)
	{
		if (nulls[i])
			continue;

		Datum		original = elems[i];
		bool		elemNull = false;

		elems[i] = ValidateDatum(elems[i], elemTypeOid, elemTypmod,
								 policy, &elemNull);

		if (elemNull)
		{
			nulls[i] = true;
			modified = true;
		}
		else if (elems[i] != original)
			modified = true;
	}

	if (!modified)
		return value;

	ArrayType  *result = construct_md_array(elems, nulls,
											ARR_NDIM(arr), ARR_DIMS(arr),
											ARR_LBOUND(arr),
											elemTypeOid, elemLen,
											elemByVal, elemAlign);

	return PointerGetDatum(result);
}


/* ================================================================
 * Composite / struct validation
 * ================================================================ */

/*
 * ValidateCompositeDatum validates fields of a composite Datum.
 */
static Datum
ValidateCompositeDatum(Datum value, Oid typeOid, OutOfRangePolicy policy)
{
	HeapTupleHeader td = DatumGetHeapTupleHeader(value);
	TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
	HeapTupleData tmptup;
	Datum	   *values;
	bool	   *nulls;
	bool		modified = false;

	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;

	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

	heap_deform_tuple(&tmptup, tupdesc, values, nulls);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped || nulls[i])
			continue;

		if (!NeedsValidation(att->atttypid, att->atttypmod))
			continue;

		Datum		original = values[i];
		bool		fieldNull = false;

		values[i] = ValidateDatum(values[i], att->atttypid, att->atttypmod,
								  policy, &fieldNull);

		if (fieldNull)
		{
			nulls[i] = true;
			modified = true;
		}
		else if (values[i] != original)
			modified = true;
	}

	if (!modified)
	{
		ReleaseTupleDesc(tupdesc);
		pfree(values);
		pfree(nulls);
		return value;
	}

	HeapTuple	newTuple = heap_form_tuple(tupdesc, values, nulls);

	ReleaseTupleDesc(tupdesc);
	pfree(values);
	pfree(nulls);

	return HeapTupleHeaderGetDatum(newTuple->t_data);
}


/* ================================================================
 * Map validation
 *
 * Maps are stored as arrays of key-value composite elements.
 * We decompose the array, validate each key/value field, and rebuild.
 * ================================================================ */

static Datum
ValidateMapDatum(Datum value, Oid mapTypeOid, OutOfRangePolicy policy)
{
	PGType		keyType = GetMapKeyType(mapTypeOid);
	PGType		valType = GetMapValueType(mapTypeOid);

	bool		keyNeeds = NeedsValidation(keyType.postgresTypeOid,
										   keyType.postgresTypeMod);
	bool		valNeeds = NeedsValidation(valType.postgresTypeOid,
										   valType.postgresTypeMod);

	if (!keyNeeds && !valNeeds)
		return value;

	/*
	 * Maps in pg_lake are domain types over arrays of (key, value)
	 * composites.  Decompose the underlying array, validate key/value fields
	 * in each composite element, and rebuild.
	 */
	Oid			baseTypeOid = getBaseType(mapTypeOid);
	Oid			elemTypeOid = get_element_type(baseTypeOid);

	if (!OidIsValid(elemTypeOid))
		return value;

	ArrayType  *arr = DatumGetArrayTypeP(value);
	int			nelems;
	Datum	   *elems;
	bool	   *elemNulls;
	int16		elemLen;
	bool		elemByVal;
	char		elemAlign;

	get_typlenbyvalalign(elemTypeOid, &elemLen, &elemByVal, &elemAlign);
	deconstruct_array(arr, elemTypeOid, elemLen, elemByVal, elemAlign,
					  &elems, &elemNulls, &nelems);

	bool		modified = false;
	TupleDesc	entryDesc = lookup_rowtype_tupdesc(elemTypeOid, -1);

	for (int i = 0; i < nelems; i++)
	{
		if (elemNulls[i])
			continue;

		HeapTupleHeader td = DatumGetHeapTupleHeader(elems[i]);
		HeapTupleData tmptup;
		Datum		entryValues[2];
		bool		entryNulls[2];
		bool		entryModified = false;

		tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
		tmptup.t_data = td;

		heap_deform_tuple(&tmptup, entryDesc, entryValues, entryNulls);

		/* validate key */
		if (keyNeeds && !entryNulls[0])
		{
			Datum		orig = entryValues[0];
			bool		kNull = false;

			entryValues[0] = ValidateDatum(entryValues[0],
										   keyType.postgresTypeOid,
										   keyType.postgresTypeMod,
										   policy, &kNull);
			if (kNull)
			{
				entryNulls[0] = true;
				entryModified = true;
			}
			else if (entryValues[0] != orig)
				entryModified = true;
		}

		/* validate value */
		if (valNeeds && !entryNulls[1])
		{
			Datum		orig = entryValues[1];
			bool		vNull = false;

			entryValues[1] = ValidateDatum(entryValues[1],
										   valType.postgresTypeOid,
										   valType.postgresTypeMod,
										   policy, &vNull);
			if (vNull)
			{
				entryNulls[1] = true;
				entryModified = true;
			}
			else if (entryValues[1] != orig)
				entryModified = true;
		}

		if (entryModified)
		{
			HeapTuple	newEntry = heap_form_tuple(entryDesc, entryValues, entryNulls);

			elems[i] = HeapTupleHeaderGetDatum(newEntry->t_data);
			modified = true;
		}
	}

	ReleaseTupleDesc(entryDesc);

	if (!modified)
		return value;

	ArrayType  *result = construct_md_array(elems, elemNulls,
											ARR_NDIM(arr), ARR_DIMS(arr),
											ARR_LBOUND(arr),
											elemTypeOid, elemLen,
											elemByVal, elemAlign);

	return PointerGetDatum(result);
}
