const path = require('node:path');
const os = require('node:os');

// Prefer an ordinary Node installation. The optional directory also permits a
// temporary npm install without modifying the application's dependencies.
function dependency(name) {
    try { return require(name); }
    catch (error) {
        if (error.code !== 'MODULE_NOT_FOUND') throw error;
        const root = process.env.PARSER_PERF_NODE_MODULES || path.join(os.homedir(),
            '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules');
        return require(path.join(root, name));
    }
}

module.exports = { dependency };
