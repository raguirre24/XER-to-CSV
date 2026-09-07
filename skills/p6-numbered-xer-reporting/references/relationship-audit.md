# Relationship allowance assessments

Use this reference when investigating `06.free_float` or a relationship whose progress state affects its meaning. The assessment uses the same signed predecessor-working-time inverse calculation as the exported value. It holds the selected successor endpoint fixed, applies the configured lag operation, and reports hours divided by positive predecessor hours/day. It does not reschedule the project or certify P6 driving status.

## Progress and endpoint matrix

N means not started; A means active. Supported endpoint types are `TT_Task`, `TT_Rsrc`, `TT_Mile` and `TT_FinMile`. `TT_Rsrc` follows the same relationship endpoint, status/progress and TASK-calendar movement rules as `TT_Task`; assigned-resource calendars remain a separate table 15 concern. All calculated cases still require unambiguous identities, valid endpoints, calendars, HPD and lag evidence.

| Predecessor -> successor | Type and exported mode | Assessment basis |
| --- | --- | --- |
| N -> N | FS, SS, FF, SF | Existing remaining endpoint mapping; progressed mode is not required. |
| A -> N | FS, FF | Predecessor remaining finish to successor remaining start or finish. |
| A -> N | SS, SF | Unsupported: predecessor actual start is a fixed event. |
| N or A -> A | FS, Retained Logic | Predecessor remaining finish to successor explicit remaining restart. |
| N or A -> A | FF, Retained Logic | Predecessor remaining finish to successor explicit remaining finish. |
| N or A -> A | SS, SF | Unsupported progressed start relationship. |
| N or A -> A | FS, Progress Override, zero lag | Ignored only when the successor has a valid actual start on or before the project Data Date. |
| N or A -> A | Other Progress Override case | Unsupported; no remaining lag or phase is invented. |
| N or A -> A | Actual Dates | Unsupported by this remaining displacement model. |
| Any completed endpoint | Any | Historical: actual events are fixed. |

When a progressed mode is required but not exported, the outcome is missing data. A malformed or contradictory required value is invalid data. Missing/invalid endpoint evidence can prevent an otherwise supported case from calculating. The audit's selected endpoint fields identify the timestamps actually used; the normal 01/06 display date policy remains separate.

The exported `sched_retained_logic` / `sched_progress_override` flags select Retained Logic with Y/N, Progress Override with N/Y, and Actual Dates with N/N. Blank/absent flags do not imply N/N. The exported `sched_lag_early_start_flag` records EarlyStart for Y and ActualStart for N; Actual Dates reports this basis as not applicable. Capturing an SS basis does not make progressed SS/SF calculations supported. Active-successor calculations also need a valid project Data Date and valid actual starts for active endpoints on or before that date. Cross-project progressed context and movement intersecting unresolved activity suspension remain explicitly unsupported.

## Reading an audit

`XerTransformer.AssessRelationships()` returns one assessment per raw TASKPRED row, including rows with no numeric allowance. The optional `XerToCsvConverter.RelationshipAudit.Cli` accepts repeated `--input` arguments and an explicit `--output <chosen-file.csv>`, with `--overwrite` for an existing audit target. The output parent folder must exist; no default filename is selected. Inputs remain ordered occurrences. Running an audit does not add normal CSVs, alter numbered schemas or extend review manifests.

Classifications are `Calculated`, `Ignored`, `Historical`, `Unsupported`, `MissingData` and `InvalidData`. Use `ReasonCode` for grouping and `Message` for explanation. Examples include `CalculatedRemainingRelationship`, `CalculatedRetainedRemainingRelationship`, `IgnoredUnderExportedProgressOverride`, `HistoricalFixedPredecessor`, `UnresolvedProgressMode` and `UnsupportedProgressedRelationship`. Only `Calculated` carries the value exported to `06.free_float`; zero is a calculated result, while every other classification stays blank. Preserve negative and subday values. The audit is relationship-only: another successor, a constraint or a resource could still restrict the predecessor in a full scheduling run. A historical predecessor can still impose a fixed release or unexpired lag; a blank movement allowance does not mean that relationship has no scheduling effect.

Identify a row by its public source namespace and one-based source TASKPRED row ordinal, with the qualified relationship ID when available. Native relationship IDs and original filenames can repeat across inputs. The internal parser occurrence token is not a durable report key and is not exposed as an audit identity.

The optional audit schema is version 1.0 with 44 columns: row provenance and assessment fields, six successor scheduling-option raw/state pairs, and an `input_evidence` JSON object for captured relationship, endpoint and project contexts. Raw input evidence preserves field distinctions: `AbsentHeader` means that occurrence never exported the column; `OmittedCell` means its row ended before the column; `Blank` is an explicitly empty cell; `Present` retains a supplied value. Typed assessment can label an unusable supplied value `Malformed`. A later input's schema must not turn an earlier absent field into an explicit blank default. Inspect the selected scheduling mode, lag-calendar/SS basis, project Data Date, endpoint fields and HPD together with the raw evidence. Import raw evidence as text.

Parsed-data callers can edit existing cells before assessment. Original field presence and raw text remain immutable evidence; evaluation uses the caller's current value for an originally present/blank cell. The audit adds `evaluation_value` and `evaluation_state` only when they differ from the original evidence. Audit evidence maps supplied values to `Valid`, or `Malformed` when a recognized typed field fails validation; untyped supplied text is also labelled `Valid`, not `Present`. That label is not proof that an untyped identifier resolves uniquely. The underlying parser still retains `Present` as its source-presence state. A padded or caller-created value cannot fabricate source evidence for an originally absent header or omitted cell. Distinguish raw source evidence from an explicit parsed-data edit when reconciling an audit.

Missing calendar identities produce `MissingData` with `UnresolvedPredecessorCalendar` or `UnresolvedLagCalendar`; a present calendar that cannot be resolved produces `InvalidData` with `InvalidPredecessorCalendar` or `InvalidLagCalendar` and its parse/inheritance failure. Nonzero lag with a missing setting is `MissingData`, while a malformed supplied setting or ambiguous options is `InvalidData`; both use `UnresolvedLagCalendarSetting`, so inspect classification and raw evidence together. A genuinely exported blank lag-calendar setting retains the successor-calendar default; absent headers and omitted cells do not.

Suspension checks use the predecessor's proposed movement interval and half-open suspension bounds: touching a suspension start or resume boundary without overlapping suspended time is not an intersection. Zero movement consumes no suspension interval. Nonzero movement with unresolved bounds remains `UnresolvedSuspensionBounds`; an intersection is `UnsupportedSuspensionMovement`. This does not model suspension as working time or perform a reschedule.

Normal Standard Enhanced exports contain the selected numbered tables and `XER_DATA_QUALITY.csv`; raw-only exports do not acquire that companion. Programme and Tender retain their ten numbered tables, companion and manifest. The companion now reports affected Enhanced fields and source evidence, including unavailable relationship calculations, but is not a full replacement for the optional audit: the audit also records every successful relationship and its complete selected calculation basis. Review date-only columns cannot reconstruct its intraday arithmetic.

## Evidence boundaries

Source reconciliation, synthetic forward-feasibility tests, file/stream equality, fixed-profile comparisons and a successful CLI run establish the implemented contract at the tested inputs. They do not certify native P6 scheduling parity, a browser-saved ZIP, report refresh or deployed UI behavior. Keep original XER files unchanged and local unless the user authorizes an upload. Do not change a report's least-float policy merely because a relationship is ignored, historical, unsupported or positive.
