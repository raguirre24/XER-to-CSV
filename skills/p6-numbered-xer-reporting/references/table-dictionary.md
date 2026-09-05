# Numbered table dictionary

The 14 legacy Enhanced tables are listed below. There is deliberately no table 05. Review profiles are projections of selected numbers, not interchangeable copies of the legacy files. Their exact headers are in [profile-contracts.md](profile-contracts.md).

Legacy CSV serialization adds the original `FileName` provenance column, not an internal token or collision-qualified public namespace. Dynamic tables preserve the deterministic union of source headers and append keys plus `MonthUpdate`; incoming-only fields are not discarded. Legacy `MonthUpdate` is inferred from the original source filename, not the P6 Data Date. Inspect the actual value instead of treating the filename convention as universal.

## Grain and joins

| Table | Source / grain | Keys and reporting use |
| --- | --- | --- |
| 01_XER_TASK | TASK; one activity per source/snapshot | `task_id_key`; project, WBS and calendar foreign keys. Activity dimension/fact at snapshot grain. |
| 02_XER_PROJECT | PROJECT; one project per source/snapshot | `proj_id_key`; P6 Data Date and project metadata. Review bundle ProjectCode is governed metadata, not the native ID. |
| 03_XER_PROJWBS | PROJWBS; one WBS node per source/snapshot | `wbs_id_key`, self-referencing `parent_wbs_id_key`; `01.wbs_id_key` joins here. Parser rejects duplicate/cyclic/cross-project parent identities; missing parents remain blank. |
| 04_XER_BASELINE | Filtered legacy 01 | All rows at the global earliest valid filename-derived MonthUpdate. Not an Oracle designated baseline, not independently selected per project. Absent from both review profiles. |
| 06_XER_PREDECESSOR | TASKPRED; one relationship | `task_id_key` is successor, `pred_task_id_key` is predecessor. Review profiles also supply `task_pred_id_key`. Multiple relationships between an activity pair can be valid. |
| 07_XER_ACTVTYPE | ACTVTYPE; one code category | `actv_code_type_id_key`; category name, not activity assignment. |
| 08_XER_ACTVCODE | ACTVCODE; one code value | `actv_code_id_key`, `actv_code_type_id_key`; join to 07 for category. |
| 09_XER_TASKACTV | TASKACTV; one activity/code assignment | Bridge `task_id_key` to `actv_code_id_key`; not an activity dimension. One activity can have multiple code assignments. |
| 10_XER_CALENDAR | CALENDAR; one calendar per source | `clndr_id_key`. Legacy preserves shifts blob, base ID and period factors; review profiles contain only key/name. |
| 11_XER_CALENDAR_DETAILED | Parsed CALENDAR; weekday rules plus dated replacements | Calendar key plus rule/day/exception identity; not unique on calendar key or a dense Date dimension. Absent from review profiles. |
| 12_XER_RSRC | RSRC; one resource per source | `rsrc_id_key`; legacy also `clndr_id_key`, `unit_id_key`. Review projection is resource key/default rate only. |
| 13_XER_TASKRSRC | TASKRSRC; one resource assignment | Raw `taskrsrc_id` remains important. Added `task_id_key`, `rsrc_id_key`; task/resource alone is not a guaranteed assignment key. Legacy only. |
| 14_XER_UMEASURE | UMEASURE; one unit definition per source | `unit_id_key`; join legacy 12. Preserve unit semantics. Legacy only. |
| 15_XER_RESOURCE_DISTRIBUTION | Derived TASKRSRC monthly units | Legacy/Programme retain assignment contributions; Tender aggregates to stage-qualified task/resource/actual/month. See calculation rules before aggregation. |

For 06, use role-playing predecessor/successor activity dimensions or deliberate measure-specific relationships so filters have an unambiguous direction. Do not make both endpoint relationships bidirectional by default. For 09, retain the bridge and avoid multiplying activity measures through code assignments. Same-name calendars/resources in separate snapshots remain separate identities unless a deliberate cross-snapshot mapping is established.

## 01: activity fields and calculations

Legacy fixed columns before the writer's FileName:

```text
task_id,proj_id,wbs_id,clndr_id,task_type,status_code,task_code,task_name,rsrc_id,act_start_date,act_end_date,early_start_date,early_end_date,late_start_date,late_end_date,target_start_date,target_end_date,cstr_type,cstr_date,priority_type,float_path,float_path_order,driving_path_flag,remain_drtn_hr_cnt,phys_complete_pct,Start,Finish,ID_Name,Remaining Duration,Original Duration,total_float,Free Float,%,Data Date,wbs_id_key,task_id_key,calendar_id_key,proj_id_key,MonthUpdate
```

Raw IDs/dates and schedule fields are imported, not a new CPM schedule. `Remaining Duration`, `Original Duration`, activity `total_float` and activity `Free Float` convert their source hour counts using the activity calendar's positive `day_hr_cnt`, with two-decimal legacy formatting. Completed activity floats are blank. The raw `remain_drtn_hr_cnt` remains hours. Do not multiply a day value by eight unless its actual calendar conversion factor is eight. Do not sum activity duration/float/percentage to represent a project duration/float/percentage.

The `%` output is 0..100, not a 0..1 fraction. Complete=100 and unstarted=0. Active physical uses source physical percent; active duration uses `(target hours - remaining hours)/target hours`; active units uses `(actual labor + actual nonlabor)/(actual labor + actual nonlabor + remaining labor + remaining nonlabor)`. The units inputs are `act_work_qty`, `act_equip_qty`, `remain_work_qty`, `remain_equip_qty`; they do not include unlike material units. Optional blank unit quantities count as zero and a zero units denominator gives zero. Physical/duration required inputs must be present, finite and nonnegative, and duration target must be positive. Malformed/negative/nonfinite/overflowing values, unknown active completion types or unknown statuses yield blank, not NaN or an invented zero. Supported results are clamped to 0..100. In Power BI, do not format an unscaled 60 as a fraction-based percent and display 6000%.

01 and the shared 06 display endpoints use status-aware dates: unstarted Start prefers `restart_date`, falling back to `early_start_date` only when the preferred field is absent; active/completed Start uses actual start. Unstarted/active Finish prefers `reend_date`, falling back to early finish only when absent; completed Finish uses actual finish. A malformed preferred value remains blank, and late dates are not forecast substitutes. Tender follows this remaining/actual policy too. Raw early/late columns remain imported raw values and can legitimately differ. `ID_Name`/`id_name` is a display label, not an identity. `driving_path_flag` is imported, not a guarantee that every relationship chosen by a custom visual is P6-driving.

## 02/03/04 and code/resource dimensions

02,03,07,08,09,10,12,13,14 retain native rows in legacy; they are not global deduplicated master dimensions. Key additions are exactly those in the grain table; most also retain other raw IDs without adding keys for every possible relationship.

03 rejects blank/duplicate WBS IDs, cycles and parent links crossing projects within a source. A genuinely absent parent is cleared to blank, preserving root/orphan display rather than inventing a node. Reports must still preserve activities missing a usable WBS mapping; parser validation does not define the report's fallback group.

04 is a legacy convenience snapshot. Do not silently replace the Programme profile's resolved baseline/adjusted-baseline history with it. If two unrelated projects have different earliest available months, global selection can omit one project's baseline entirely. No valid filename month produces a header-only 04, not an old file retained from another export.

07 -> 08 -> 09 provides code category -> code value -> activity mapping. Labels can repeat; use qualified keys. 12's rate is not a monthly quantity. 13 preserves raw assignment quantities and profiles (`remain_crv`, `curv_id`) supplied by any input schema. Missing values for an occurrence are not borrowed from another occurrence. 14 identifies units, not a universal conversion between unlike resources.

## 06: relationship fields

Legacy preserves TASKPRED fields and adds:

```text
task_id_key,pred_task_id_key,calendar_id_key,predecessor_clndr_id_key,status_code,predecessor_status_code,task_type,predecessor_task_type,lag,time_period_hours_per_day,Start,Finish,predecessor_start,predecessor_finish,free_float,total_float,MonthUpdate
```

`free_float` is derived per relationship in predecessor-calendar day units. `total_float` repeats successor activity float converted with the successor's valid HPD; it remains blank when that conversion is unresolved, never defaulting to eight. `lag` is a display conversion of raw lag hours using predecessor HPD; that does not define which calendar schedules the lag. `calendar_id_key` denotes successor calendar; `predecessor_clndr_id_key` denotes predecessor calendar. Display dates follow the 01 status-aware policy above; 15 dates remain assignment-period dates. Review date-only fields still lose intraday precision and cannot reconstruct relationship arithmetic.

Use explicit FS/SS/FF/SF type mapping; never infer every row to be FS. Endpoint resolution requires exactly one same-source task identity and matching exported project context; a project filter cannot legitimize duplicate task IDs. An unresolved external predecessor is not linked to a local same-ID activity. Standard leaves unresolved endpoint keys blank; review profiles validate their raw endpoints and reject invalid bundle edges. Keep unknown free float nullable and visible in data-quality counts.

## 10 and 11: calendar data

10's legacy `day_hr_cnt`, `week_hr_cnt`, `month_hr_cnt`, `year_hr_cnt` are conversion factors. `clndr_data` defines work availability; `base_clndr_id` can supply inherited exceptions. The fixed review 10 has neither these factors nor shifts, so a report cannot reconstruct a full working calendar from that projection alone.

11 columns before FileName:

```text
clndr_id,clndr_name,clndr_type,date,day_of_week,working_day,work_hours,exception_type,clndr_id_key,MonthUpdate,day_of_week_num,working_day_int
```

Seven standard-weekday rows have blank `date`; additional dated rows replace that date's rule, including inherited/overridden exceptions. `day_of_week_num` is Monday=1 through Sunday=7 (different from the raw P6 weekday numbering). `work_hours` is daily resolved availability; the table does not expose exact intraday intervals. To create a calendar-date bridge, expand the reporting date range per qualified calendar and choose its dated exception if present, otherwise the weekday rule. Never add exception hours to weekday hours.

## 15: distribution fields

Legacy columns before FileName:

```text
task_id_key,rsrc_id_key,clndr_id_key,proj_id_key,distribution_month,month_start_date,month_end_date,monthly_quantity,distribution_type,month_working_hours,total_working_hours,calendar_hours_per_day,month_working_days,total_working_days,month_calendar_days,total_calendar_days,Start,Finish,is_actual,status_code,Unit,task_code,rsrc_short_name,rsrc_name,rsrc_type,MonthUpdate
```

`distribution_month` is the monthly bucket date, independent of the source's update/status date. `Start`/`Finish` are allocation-period timestamps, not necessarily activity endpoints. `is_actual` separates actual from remaining. `distribution_type` distinguishes `Working Hours`, `Resource Curve`, `Remaining Units Profile` in legacy; it is not included in the review projection.

Legacy nonmaterial unit labels may look like units/time. The allocated value is nevertheless assignment units, not a rate multiplied by time; inspect source resource/unit definitions before choosing display labels or summing unlike resources.

`monthly_quantity` is additive across compatible assignment/month contributions within the intended snapshot. `total_working_hours`, `total_working_days`, `total_calendar_days`, conversion factors and period endpoints repeat across monthly rows; do not sum them. Even monthly working hours are assignment exposure, not distinct resource capacity: overlapping assignments can cover the same hours. `month_working_days` is working hours divided by HPD, not a count of distinct dates. Curve shapes change units, not these raw calendar diagnostics. No profile exports an assignment ID in table 15, and review projections also omit diagnostics. Visible duplicate task/resource/month rows in Standard or Programme can be legitimate separate assignments. Reconstruct from raw assignments for aggregate or multiset reconciliation; these CSV rows alone do not provide unique assignment traceability.
