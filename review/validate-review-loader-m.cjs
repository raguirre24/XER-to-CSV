"use strict";

// Syntax validation only: this never executes M or connects to a data source.
const fs = require("node:fs");
const path = require("node:path");

async function main() {
    const parserDirectory = process.argv[2];
    if (!parserDirectory) throw new Error("Pass the installed Microsoft Power Query parser directory.");
    const parser = require(path.resolve(parserDirectory));
    const input = JSON.parse(fs.readFileSync(0, "utf8").replace(/^\uFEFF/, ""));
    const failures = [];
    const sharedNamesByModel = new Map();
    for (const query of input.queries) {
        if (query.kind !== "shared") continue;
        if (!sharedNamesByModel.has(query.model)) sharedNamesByModel.set(query.model, new Set());
        sharedNamesByModel.get(query.model).add(query.name);
    }
    for (const query of input.queries) {
        const result = await parser.TaskUtils.tryLexParse(parser.DefaultSettings, query.expression);
        if (parser.TaskUtils.isError(result)) {
            failures.push({ model: query.model, query: query.name, kind: query.kind, message: result.error?.message || String(result.error) });
            continue;
        }
        // Check actual identifier tokens, avoiding incidental names in comments or text.
        const identifiers = result.lexerSnapshot.tokens
            .filter(token => token.kind === "Identifier")
            .map(token => token.data)
            .filter(name => /^fn(?:XerCsv|TenderCsv)[A-Za-z0-9_]*$/.test(name));
        for (const identifier of new Set(identifiers)) {
            if (!sharedNamesByModel.get(query.model)?.has(identifier)) {
                failures.push({ model: query.model, query: query.name, kind: query.kind, message: `Undefined CSV helper: ${identifier}` });
            }
        }
    }
    console.log(JSON.stringify({
        models: input.models,
        mParserVersion: require(path.join(path.resolve(parserDirectory), "package.json")).version,
        parsedExpressions: input.queries.length,
        result: failures.length === 0 ? "PASS" : "FAIL",
        failures,
        scope: "TOM deserialization, Microsoft M syntax parsing, and CSV helper reference checks; no M evaluation, data refresh, SharePoint access, or Power BI host validation."
    }, null, 2));
    if (failures.length) process.exitCode = 1;
}

main().catch(error => {
    console.error(error.message);
    process.exitCode = 1;
});
