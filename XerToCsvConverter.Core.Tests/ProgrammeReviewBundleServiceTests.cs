using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using XerToCsvConverter.ProgrammeReview;

namespace XerToCsvConverter.Core.Tests;

public sealed class ProgrammeReviewBundleServiceTests
{
    [Fact]
    public async Task Build_publishes_exact_complete_bundle_and_manifest_cartesian_rows()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "baseline source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            ProgrammeReviewSnapshot update = ProgrammeReviewNamingTests.Snapshot(
                "2602 update source.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-27") with
            {
                UpdateDate = new DateOnly(2026, 2, 28)
            };
            ProgrammeReviewSnapshot update2 = ProgrammeReviewNamingTests.Snapshot(
                "2603 update source.xer", ProgrammeReviewSnapshotKind.Update, "2603", "2026-03-01", "2026-03-27") with
            {
                UpdateDate = new DateOnly(2026, 3, 31)
            };
            ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline, update, update2 });
            XerDataStore store = BuildDataStore(
                (baseline, "2026-02-02", "2026-02-06"),
                (update, "2026-02-04", "2026-02-10"),
                (update2, "2026-02-05", "2026-02-13"));

            ProgrammeReviewBundleResult result = await new ProgrammeReviewBundleService()
                .BuildFromParsedDataAsync(store, request, root);

            Assert.True(Directory.Exists(result.BundlePath));
            Assert.Equal(11, Directory.EnumerateFiles(result.BundlePath).Count());
            Assert.Equal(30, result.ManifestRows.Count);
            Assert.All(result.ManifestRows, row => Assert.Equal("complete", row.BundleStatus));
            Assert.All(result.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));
            Assert.Equal(1, result.ManifestRows.Single(r => r.TableName == "01_XER_TASK" && r.OriginalXerFilename == baseline.OriginalXerFilename).RowCount);
            Assert.Equal(0, result.ManifestRows.Single(r => r.TableName == "07_XER_ACTVTYPE" && r.OriginalXerFilename == baseline.OriginalXerFilename).RowCount);

            string taskPath = Path.Combine(result.BundlePath, "01_XER_TASK.csv");
            string[] lines = File.ReadAllLines(taskPath);
            Assert.Equal(4, lines.Length);
            string[] headers = lines[0].Split(',');
            Assert.Equal(47, headers.Length);
            string[] updateValues = lines.Single(line => line.Contains("J123-C-2602_20260227.xer", StringComparison.Ordinal)).Split(',');
            var row = headers.Zip(updateValues).ToDictionary(pair => pair.First, pair => pair.Second);
            Assert.Equal("2026-02-06", row["Baseline Finish"]);
            Assert.Equal("2026-02-06", row["Previous Month Finish"]);
            Assert.Equal("CSV::J123::C::2602::T2", row["task_id_key"]);
            Assert.DoesNotContain('|', row["task_id_key"]);
            string[] update2Values = lines.Single(line => line.Contains("J123-C-2603_20260327.xer", StringComparison.Ordinal)).Split(',');
            var row2 = headers.Zip(update2Values).ToDictionary(pair => pair.First, pair => pair.Second);
            Assert.Equal("2026-02-10", row2["Previous Month Finish"]);

            foreach ((string file, string sha) in result.CsvSha256ByFile)
                Assert.Equal(sha, Sha256(Path.Combine(result.BundlePath, file)));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task In_memory_build_returns_exact_browser_bundle_without_file_system_output()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "baseline source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
        XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));

        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(11, result.Files.Count);
        Assert.Equal(
            ProgrammeReviewContract.Tables.Select(table => table.FileName)
                .Append(ProgrammeReviewContract.ManifestFileName)
                .Order(StringComparer.Ordinal),
            result.Files.Keys.Order(StringComparer.Ordinal));
        Assert.StartsWith("schema_version,bundle_id,bundle_status", Encoding.UTF8.GetString(
            result.Files[ProgrammeReviewContract.ManifestFileName]), StringComparison.Ordinal);
        foreach ((string file, string expectedHash) in result.CsvSha256ByFile)
        {
            string actualHash = Convert.ToHexString(SHA256.HashData(result.Files[file])).ToLowerInvariant();
            Assert.Equal(expectedHash, actualHash);
        }
    }

    [Fact]
    public async Task In_memory_build_is_byte_deterministic_for_the_same_request()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "baseline source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
        XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
        var service = new ProgrammeReviewBundleService();

        ProgrammeReviewInMemoryBundleResult first = await service.BuildFromParsedDataToMemoryAsync(store, request);
        ProgrammeReviewInMemoryBundleResult second = await service.BuildFromParsedDataToMemoryAsync(store, request);

        Assert.Equal(first.BundleId, second.BundleId);
        Assert.Equal(first.Files.Keys.Order(StringComparer.Ordinal), second.Files.Keys.Order(StringComparer.Ordinal));
        foreach (string file in first.Files.Keys)
            Assert.Equal(first.Files[file], second.Files[file]);
    }

    [Fact]
    public async Task Regenerated_bundle_keeps_every_csv_relationship_key_stable_across_export_times()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "baseline source.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
        ProgrammeReviewSnapshot update = ProgrammeReviewNamingTests.Snapshot(
            "2602 update source.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-27") with
        {
            UpdateDate = new DateOnly(2026, 2, 28)
        };
        ProgrammeReviewBundleRequest firstRequest = ProgrammeReviewNamingTests.Request(new[] { baseline, update });
        ProgrammeReviewBundleRequest secondRequest = firstRequest with
        {
            ExportedAtUtc = firstRequest.ExportedAtUtc!.Value.AddDays(1)
        };
        XerDataStore store = BuildDataStore(
            (baseline, "2026-02-02", "2026-02-06"),
            (update, "2026-02-04", "2026-02-10"));
        var service = new ProgrammeReviewBundleService();

        ProgrammeReviewInMemoryBundleResult first = await service.BuildFromParsedDataToMemoryAsync(store, firstRequest);
        ProgrammeReviewInMemoryBundleResult second = await service.BuildFromParsedDataToMemoryAsync(store, secondRequest);

        Assert.NotEqual(first.BundleId, second.BundleId);
        foreach (ProgrammeReviewTableContract table in ProgrammeReviewContract.Tables)
        {
            byte[] firstCsv = first.Files[table.FileName];
            byte[] secondCsv = second.Files[table.FileName];
            Assert.Equal(firstCsv, secondCsv);

            IReadOnlyDictionary<string, IReadOnlyList<string>> firstKeys = ReadRelationshipKeys(firstCsv);
            IReadOnlyDictionary<string, IReadOnlyList<string>> secondKeys = ReadRelationshipKeys(secondCsv);
            Assert.Equal(firstKeys.Keys.Order(StringComparer.Ordinal), secondKeys.Keys.Order(StringComparer.Ordinal));
            foreach (string keyColumn in firstKeys.Keys)
                Assert.Equal(firstKeys[keyColumn], secondKeys[keyColumn]);
        }
    }

    [Fact]
    public async Task Browser_byte_entrypoint_hashes_and_parses_uploaded_xer_content()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "browser baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30") with
        {
            SourceSha256 = null
        };
        ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
        byte[] xer = MinimalXerBytes(baseline.DataDate, "2026-02-02", "2026-02-06");
        var uploads = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [baseline.OriginalXerFilename] = xer
        };

        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromXerBytesAsync(request, uploads);

        Assert.Equal(11, result.Files.Count);
        Assert.Equal(10, result.ManifestRows.Count);
        string expectedSourceHash = Convert.ToHexString(SHA256.HashData(xer)).ToLowerInvariant();
        Assert.All(result.ManifestRows, row => Assert.Equal(expectedSourceHash, row.SourceSha256));
        Assert.Single(Encoding.UTF8.GetString(result.Files["01_XER_TASK.csv"])
            .Split("\r\n", StringSplitOptions.RemoveEmptyEntries).Skip(1));
    }

    [Fact]
    public async Task Browser_byte_entrypoint_detects_input_mutation_without_cloning_history()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "browser baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30") with
        {
            SourceSha256 = null
        };
        byte[] xer = MinimalXerBytes(baseline.DataDate, "2026-02-02", "2026-02-06");
        var uploads = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [baseline.OriginalXerFilename] = xer
        };
        bool mutated = false;
        var progress = new InlineProgress<ProcessingService.DetailedProgress>(value =>
        {
            if (mutated || value.Message?.StartsWith("Finished parsing", StringComparison.Ordinal) != true)
                return;

            xer[^1] ^= 1;
            mutated = true;
        });

        ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
            new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
                ProgrammeReviewNamingTests.Request(new[] { baseline }), uploads, progress));

        Assert.True(mutated);
        Assert.Contains("changed while it was being parsed", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Browser_byte_entrypoint_rejects_duplicate_source_content()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        ProgrammeReviewSnapshot update = ProgrammeReviewNamingTests.Snapshot(
            "update.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        byte[] duplicate = MinimalXerBytes(baseline.DataDate, "2026-02-02", "2026-02-06");
        var uploads = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [baseline.OriginalXerFilename] = duplicate,
            [update.OriginalXerFilename] = duplicate
        };

        ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
            new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
                ProgrammeReviewNamingTests.Request(new[] { baseline, update }), uploads));

        Assert.Contains("identical source content", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Browser_byte_entrypoint_requires_case_exact_uploaded_keys()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "Baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        var uploads = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["baseline.xer"] = MinimalXerBytes(baseline.DataDate, "2026-02-02", "2026-02-06")
        };

        ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
            new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
                ProgrammeReviewNamingTests.Request(new[] { baseline }), uploads));

        Assert.Contains("exactly match", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Browser_byte_entrypoint_honors_pre_cancellation()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        var uploads = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [baseline.OriginalXerFilename] = MinimalXerBytes(baseline.DataDate, "2026-02-02", "2026-02-06")
        };
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
                ProgrammeReviewNamingTests.Request(new[] { baseline }), uploads, cancellationToken: cancellation.Token));
    }

    [Fact]
    public async Task Browser_byte_entrypoint_marks_only_resolved_history_in_manifest()
    {
        ProgrammeReviewSnapshot olderBaseline = ProgrammeReviewNamingTests.Snapshot(
            "base-a.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01-A", "2026-01-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        ProgrammeReviewSnapshot selectedBaseline = ProgrammeReviewNamingTests.Snapshot(
            "base-b.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01-B", "2026-02-01", "2026-02-27") with
        {
            SourceSha256 = null
        };
        ProgrammeReviewSnapshot preAnchor = ProgrammeReviewNamingTests.Snapshot(
            "old.xer", ProgrammeReviewSnapshotKind.Update, "2601", "2026-01-01", "2026-01-30") with
        {
            SourceSha256 = null
        };
        ProgrammeReviewSnapshot retainedUpdate = ProgrammeReviewNamingTests.Snapshot(
            "kept.xer", ProgrammeReviewSnapshotKind.Update, "2603", "2026-03-01", "2026-03-27") with
        {
            SourceSha256 = null
        };
        var uploads = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [olderBaseline.OriginalXerFilename] = MinimalXerBytes(olderBaseline.DataDate, "2026-02-02", "2026-02-06", "Older baseline"),
            [selectedBaseline.OriginalXerFilename] = MinimalXerBytes(selectedBaseline.DataDate, "2026-02-02", "2026-02-06", "Selected baseline"),
            [preAnchor.OriginalXerFilename] = MinimalXerBytes(preAnchor.DataDate, "2026-02-02", "2026-02-06", "Pre anchor"),
            [retainedUpdate.OriginalXerFilename] = MinimalXerBytes(retainedUpdate.DataDate, "2026-03-02", "2026-03-06", "Retained update")
        };

        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService().BuildFromXerBytesAsync(
            ProgrammeReviewNamingTests.Request(new[] { olderBaseline, selectedBaseline, preAnchor, retainedUpdate }), uploads);

        Assert.Equal(
            new[] { selectedBaseline.OriginalXerFilename, retainedUpdate.OriginalXerFilename }.Order(StringComparer.Ordinal),
            result.ManifestRows.Select(row => row.OriginalXerFilename).Distinct().Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Validation_failure_removes_staging_and_never_publishes_final_bundle()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[]
            {
                ProgrammeReviewNamingTests.Snapshot("base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-31")
            });
            await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(new XerDataStore(), request, root));
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Existing_optional_raw_rows_cannot_be_silently_replaced_by_header_only_output()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            // This deliberate duplicate of a generated output header makes the legacy 06 transformer fail and return null.
            XerTable malformed = NewTable("TASKPRED", new[] { "task_id_key" });
            malformed.AddRow(new DataRow(new[] { "broken" }, baseline.OriginalXerFilename));
            store.AddTable(malformed);

            ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(store, request, root));
            Assert.Contains("06_XER_PREDECESSOR", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Raw_field_order_does_not_change_bundle_generation()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            ReverseHeaders(store, "TASK");

            ProgrammeReviewBundleResult result = await new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                store, ProgrammeReviewNamingTests.Request(new[] { baseline }), root);

            Assert.Single(File.ReadLines(Path.Combine(result.BundlePath, "01_XER_TASK.csv")).Skip(1));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Missing_required_native_field_fails_without_publishing()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            RemoveColumn(store, "TASK", "clndr_id");

            ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                    store, ProgrammeReviewNamingTests.Request(new[] { baseline }), root));
            Assert.Contains("calendar_id_key", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Multi_project_XER_is_rejected_instead_of_silently_filtered()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            store.GetTable("PROJECT")!.AddRow(new DataRow(new[] { "P2", "2026-01-30" }, baseline.OriginalXerFilename));

            ProgrammeReviewValidationException error = await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                    store, ProgrammeReviewNamingTests.Request(new[] { baseline }), root));
            Assert.Contains("exactly one native PROJECT", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Precancelled_build_leaves_no_staging_directory()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                    store, ProgrammeReviewNamingTests.Request(new[] { baseline }), root, cancellation.Token));
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Existing_complete_bundle_is_not_overwritten_or_partially_replaced()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"));
            ProgrammeReviewBundleResult first = await new ProgrammeReviewBundleService()
                .BuildFromParsedDataAsync(store, request, root);
            string manifestBefore = File.ReadAllText(Path.Combine(first.BundlePath, ProgrammeReviewContract.ManifestFileName));

            await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                new ProgrammeReviewBundleService().BuildFromParsedDataAsync(store, request, root));

            Assert.Equal(11, Directory.EnumerateFiles(first.BundlePath).Count());
            Assert.Equal(manifestBefore, File.ReadAllText(Path.Combine(first.BundlePath, ProgrammeReviewContract.ManifestFileName)));
            Assert.Single(Directory.EnumerateDirectories(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Task_history_matching_is_case_sensitive_like_Athena()
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "base.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            ProgrammeReviewSnapshot update = ProgrammeReviewNamingTests.Snapshot(
                "update.xer", ProgrammeReviewSnapshotKind.Update, "2602", "2026-02-01", "2026-02-27") with
            {
                UpdateDate = new DateOnly(2026, 2, 28)
            };
            XerDataStore store = BuildDataStore((baseline, "2026-02-02", "2026-02-06"), (update, "2026-02-04", "2026-02-10"));
            XerTable task = store.GetTable("TASK")!;
            int taskCode = task.FieldIndexes["task_code"];
            task.Rows.Single(r => r.SourceFilename == update.OriginalXerFilename).Fields[taskCode] = "a100";

            ProgrammeReviewBundleResult result = await new ProgrammeReviewBundleService().BuildFromParsedDataAsync(
                store, ProgrammeReviewNamingTests.Request(new[] { baseline, update }), root);
            string[] lines = File.ReadAllLines(Path.Combine(result.BundlePath, "01_XER_TASK.csv"));
            string[] headers = lines[0].Split(',');
            string[] updateValues = lines.Single(line => line.Contains("J123-C-2602_20260227.xer", StringComparison.Ordinal)).Split(',');
            var row = headers.Zip(updateValues).ToDictionary(pair => pair.First, pair => pair.Second);

            Assert.Equal("", row["Baseline Finish"]);
            Assert.Equal("2026-02-10", row["Previous Month Finish"]);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Resource_distribution_preserves_conserved_monthly_quantities_and_existing_profile_schema()
    {
        ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
            "resource baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
        XerDataStore store = BuildDataStore((baseline, "2026-01-31 00:00", "2026-03-02 00:00"));
        AddContinuousCalendarResourceAssignments(store, baseline.OriginalXerFilename, "1");

        ProgrammeReviewInMemoryBundleResult result = await new ProgrammeReviewBundleService()
            .BuildFromParsedDataToMemoryAsync(store, ProgrammeReviewNamingTests.Request(new[] { baseline }));
        IReadOnlyList<string[]> csv = ReadCsv(result.Files["15_XER_RESOURCE_DISTRIBUTION.csv"]);
        Assert.Equal(
            "task_id_key,rsrc_id_key,is_actual,distribution_month,monthly_quantity,rsrc_name,rsrc_type,unit,ProjectCode",
            string.Join(',', csv[0]));
        Dictionary<string, string>[] rows = csv.Skip(1).Select(values => csv[0].Zip(values)
            .ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal)).ToArray();

        Assert.Equal(new[] { "2026-01-01", "2026-02-01", "2026-03-01" },
            rows.Select(row => row["distribution_month"]));
        Assert.Equal(new[] { 0.0333m, 0.9334m, 0.0333m },
            rows.Select(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.Equal(1m, rows.Sum(row => decimal.Parse(row["monthly_quantity"], CultureInfo.InvariantCulture)));
        Assert.All(rows, row =>
        {
            Assert.Equal("CSV::J123::C::BL01::T1", row["task_id_key"]);
            Assert.Equal("CSV::J123::C::BL01::R1", row["rsrc_id_key"]);
        });
        Assert.Equal("clndr_id_key,clndr_name", string.Join(',', ReadCsv(result.Files["10_XER_CALENDAR.csv"])[0]));
        Assert.DoesNotContain("11_XER_CALENDAR_DETAILED.csv", result.Files.Keys);
        Assert.Equal(11, result.Files.Count);
        Assert.All(result.ManifestRows, row => Assert.Equal("3.0", row.SchemaVersion));
        Assert.Equal(3, result.ManifestRows.Single(row => row.TableName == "15_XER_RESOURCE_DISTRIBUTION").RowCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Invalid_positive_resource_assignment_rejects_partial_profile_output(bool toDisk)
    {
        string root = NewTempDirectory();
        try
        {
            ProgrammeReviewSnapshot baseline = ProgrammeReviewNamingTests.Snapshot(
                "resource baseline.xer", ProgrammeReviewSnapshotKind.Baseline, "BL01", "2026-01-31", "2026-01-30");
            XerDataStore store = BuildDataStore((baseline, "2026-01-31 00:00", "2026-03-02 00:00"));
            AddContinuousCalendarResourceAssignments(store, baseline.OriginalXerFilename, "1", "2");
            XerTable assignments = store.GetTable("TASKRSRC")!;
            assignments.Rows[1].Fields[assignments.FieldIndexes["restart_date"]] = "not-a-date";
            ProgrammeReviewBundleRequest request = ProgrammeReviewNamingTests.Request(new[] { baseline });
            var service = new ProgrammeReviewBundleService();

            ProgrammeReviewValidationException error = toDisk
                ? await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                    service.BuildFromParsedDataAsync(store, request, root))
                : await Assert.ThrowsAsync<ProgrammeReviewValidationException>(() =>
                    service.BuildFromParsedDataToMemoryAsync(store, request));

            Assert.Contains("15_XER_RESOURCE_DISTRIBUTION", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFileSystemEntries(root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static void AddContinuousCalendarResourceAssignments(
        XerDataStore store, string source, params string[] quantities)
    {
        XerTable calendar = store.GetTable("CALENDAR")!;
        calendar.Rows[0].Fields[calendar.FieldIndexes["day_hr_cnt"]] = "24";
        calendar.Rows[0].Fields[calendar.FieldIndexes["clndr_data"]] = P6TestCalendars.WorkWeek("24");
        var resource = NewTable("RSRC", new[]
        {
            "rsrc_id", "rsrc_short_name", "rsrc_name", "rsrc_type", "clndr_id", "def_qty_per_hr"
        });
        resource.AddRow(new DataRow(new[] { "R1", "LAB", "Labour", "RT_Labor", "C1", "1" }, source));
        store.AddTable(resource);
        var assignments = NewTable("TASKRSRC", new[]
        {
            "taskrsrc_id", "task_id", "proj_id", "rsrc_id", "act_reg_qty", "act_ot_qty", "remain_qty",
            "act_start_date", "act_end_date", "restart_date", "reend_date"
        });
        for (int index = 0; index < quantities.Length; index++)
            assignments.AddRow(new DataRow(new[]
            {
                $"A{index + 1}", "T1", "P1", "R1", "0", "0", quantities[index], "", "",
                "2026-01-31 00:00", "2026-03-02 00:00"
            }, source));
        store.AddTable(assignments);
    }

    private static IReadOnlyList<string[]> ReadCsv(byte[] content)
    {
        using var stream = new MemoryStream(content, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");
        var rows = new List<string[]>();
        while (!parser.EndOfData)
            rows.Add(parser.ReadFields() ?? throw new InvalidDataException("Unreadable Programme CSV row."));
        return rows;
    }

    private static XerDataStore BuildDataStore(params (ProgrammeReviewSnapshot Snapshot, string Start, string Finish)[] snapshots)
    {
        var store = new XerDataStore();
        var task = NewTable("TASK", new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
            "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt",
            "total_float_hr_cnt", "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"
        });
        var project = NewTable("PROJECT", new[] { "proj_id", "last_recalc_date" });
        var wbs = NewTable("PROJWBS", new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" });
        var calendar = NewTable("CALENDAR", new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" });

        int id = 0;
        foreach ((ProgrammeReviewSnapshot snapshot, string start, string finish) in snapshots)
        {
            id++;
            string source = snapshot.OriginalXerFilename;
            task.AddRow(new DataRow(new[]
            {
                $"T{id}", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", "Test activity",
                "", "", "", start, finish, start, finish, start, finish, "", "", "", "", "", "Y",
                "24", "40", "16", "8", "CP_Phys", "0", "0", "0"
            }, source));
            project.AddRow(new DataRow(new[] { "P1", snapshot.DataDate.ToString("yyyy-MM-dd") }, source));
            wbs.AddRow(new DataRow(new[] { "W1", "", "P1", "Root WBS" }, source));
            calendar.AddRow(new DataRow(new[] { "C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek() }, source));
        }

        store.AddTable(task);
        store.AddTable(project);
        store.AddTable(wbs);
        store.AddTable(calendar);
        return store;
    }

    private static byte[] MinimalXerBytes(
        DateOnly dataDate,
        string start,
        string finish,
        string activityName = "Browser activity")
    {
        var builder = new StringBuilder();
        AppendXerTable(builder, "TASK", new[]
        {
            "task_id", "proj_id", "wbs_id", "clndr_id", "task_type", "status_code", "task_code", "task_name",
            "rsrc_id", "act_start_date", "act_end_date", "early_start_date", "early_end_date", "late_start_date",
            "late_end_date", "target_start_date", "target_end_date", "cstr_type", "cstr_date", "priority_type",
            "float_path", "float_path_order", "driving_path_flag", "remain_drtn_hr_cnt", "target_drtn_hr_cnt",
            "total_float_hr_cnt", "free_float_hr_cnt", "complete_pct_type", "phys_complete_pct", "act_work_qty", "remain_work_qty"
        }, new[]
        {
            "T1", "P1", "W1", "C1", "TT_Task", "TK_NotStart", "A100", activityName,
            "", "", "", start, finish, start, finish, start, finish, "", "", "", "", "", "Y",
            "24", "40", "16", "8", "CP_Phys", "0", "0", "0"
        });
        AppendXerTable(builder, "PROJECT",
            new[] { "proj_id", "last_recalc_date" },
            new[] { "P1", dataDate.ToString("yyyy-MM-dd") });
        AppendXerTable(builder, "PROJWBS",
            new[] { "wbs_id", "parent_wbs_id", "proj_id", "wbs_name" },
            new[] { "W1", "", "P1", "Root WBS" });
        AppendXerTable(builder, "CALENDAR",
            new[] { "clndr_id", "clndr_name", "day_hr_cnt", "clndr_type", "clndr_data" },
            new[] { "C1", "Standard 8h", "8", "CA_Project", P6TestCalendars.WorkWeek() });
        builder.Append("%E\r\n");
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static void AppendXerTable(StringBuilder builder, string name, string[] headers, string[] values)
    {
        builder.Append("%T\t").Append(name).Append("\r\n");
        builder.Append("%F\t").AppendJoin('\t', headers).Append("\r\n");
        builder.Append("%R\t").AppendJoin('\t', values).Append("\r\n");
    }

    private static XerTable NewTable(string name, string[] headers)
    {
        var table = new XerTable(name);
        table.SetHeaders(headers);
        return table;
    }

    private static void ReverseHeaders(XerDataStore store, string tableName)
    {
        XerTable source = store.GetTable(tableName)!;
        string[] headers = source.Headers!.Reverse().ToArray();
        var reordered = NewTable(tableName, headers);
        foreach (DataRow row in source.Rows)
            reordered.AddRow(new DataRow(headers.Select(header => row.Fields[source.FieldIndexes[header]]).ToArray(), row.SourceFilename));
        store.AddTable(reordered);
    }

    private static void RemoveColumn(XerDataStore store, string tableName, string removedColumn)
    {
        XerTable source = store.GetTable(tableName)!;
        string[] headers = source.Headers!.Where(header => !string.Equals(header, removedColumn, StringComparison.OrdinalIgnoreCase)).ToArray();
        var reduced = NewTable(tableName, headers);
        foreach (DataRow row in source.Rows)
            reduced.AddRow(new DataRow(headers.Select(header => row.Fields[source.FieldIndexes[header]]).ToArray(), row.SourceFilename));
        store.AddTable(reduced);
    }

    private static string Sha256(string path)
    {
        using FileStream stream = File.OpenRead(path);
        return Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
    }

    private static IReadOnlyDictionary<string, IReadOnlyList<string>> ReadRelationshipKeys(byte[] csv)
    {
        using var stream = new MemoryStream(csv, writable: false);
        using var parser = new TextFieldParser(stream, Encoding.UTF8, detectEncoding: false)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(",");

        string[] headers = parser.ReadFields()
            ?? throw new InvalidDataException("Programme Review CSV is missing its header row.");
        int[] keyIndexes = headers
            .Select((header, index) => (header, index))
            .Where(item => item.header.EndsWith("_key", StringComparison.OrdinalIgnoreCase))
            .Select(item => item.index)
            .ToArray();
        var values = keyIndexes.ToDictionary(
            index => headers[index],
            _ => new List<string>(),
            StringComparer.Ordinal);

        while (!parser.EndOfData)
        {
            string[] fields = parser.ReadFields()
                ?? throw new InvalidDataException("Programme Review CSV contains an unreadable row.");
            Assert.Equal(headers.Length, fields.Length);
            foreach (int index in keyIndexes)
                values[headers[index]].Add(fields[index]);
        }

        return values.ToDictionary(
            pair => pair.Key,
            pair => (IReadOnlyList<string>)pair.Value,
            StringComparer.Ordinal);
    }

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), "XerToCsvConverter.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
