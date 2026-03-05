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

#pragma once

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

/*
 * Behavior for out-of-range values during writes.
 * Applies to temporal types (date, timestamp, timestamptz) and
 * numeric types (NaN/Inf, unbounded precision overflow).
 *
 * Controlled by the out_of_range_values table option or COPY option.
 *
 * NONE skips validation entirely (used for non-Iceberg/Parquet tables
 * such as heap tables loaded via CREATE TABLE ... WITH (load_from=...)).
 */
typedef enum OutOfRangePolicy
{
	OUT_OF_RANGE_NONE = 0,
	OUT_OF_RANGE_ERROR = 1,
	OUT_OF_RANGE_CLAMP = 2,
}			OutOfRangePolicy;

/*
 * GetOutOfRangePolicyFromOptions returns the OutOfRangePolicy
 * from the given option list.  Returns OUT_OF_RANGE_CLAMP if
 * the option is not present.
 */
extern PGDLLEXPORT OutOfRangePolicy GetOutOfRangePolicyFromOptions(List *options);

/*
 * GetOutOfRangePolicyForTable returns the OutOfRangePolicy
 * from the given table's foreign table options.
 */
extern PGDLLEXPORT OutOfRangePolicy GetOutOfRangePolicyForTable(Oid relationId);

/*
 * ValidateTemporalDatum validates a date, timestamp, or timestamptz
 * Datum for out-of-range or infinity values.  Returns the original
 * value if it is in range, or a clamped/error value otherwise.
 */
extern PGDLLEXPORT Datum ValidateTemporalDatum(Datum value, Oid typeOid,
											   OutOfRangePolicy policy);

/*
 * ValidateDatum validates a single Datum value for out-of-range
 * temporal or numeric issues before writing to Parquet.
 *
 * Handles scalars, arrays, structs, and maps recursively.
 * *isNull may be set to true when clamping NaN to NULL.
 */
extern PGDLLEXPORT Datum ValidateDatum(Datum value, Oid typeOid, int32 typmod,
									   OutOfRangePolicy policy, bool *isNull);

/*
 * TupleDescNeedsValidation returns true if any column in the tuple
 * descriptor has a type that needs out-of-range validation.
 */
extern PGDLLEXPORT bool TupleDescNeedsValidation(TupleDesc tupleDesc,
												 OutOfRangePolicy policy);
