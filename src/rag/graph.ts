import { Pool } from 'pg';
import { ChatOpenAI } from '@langchain/openai';
import {
    Annotation,
    StateGraph,
} from '@langchain/langgraph';
import * as dotenv from 'dotenv';
dotenv.config();

// Environment
const PG_CONNECTION_STRING = process.env.PG_CONNECTION_STRING || process.env.DATABASE_URL;
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

if (!process.env.OPENAI_API_KEY) {
    // We don't throw here to allow unit tests without LLM, but warn.
    console.warn('[graph] OPENAI_API_KEY is not set. SQL generation will fail.');
}

// Attempt to auto-qualify tables in FROM/JOIN when unique mapping exists in allowedTables
function qualifySqlTables(sql: string, allowedTables: string[]): string {
    // Build map from bare table name -> schema.table when unique
    const map = new Map<string, string>();
    const lowerAllowed = allowedTables.map(t => t.toLowerCase());
    const tableToSchemas = new Map<string, string[]>();
    for (const fq of lowerAllowed) {
        const [schema, tbl] = fq.split('.');
        const arr = tableToSchemas.get(tbl) || [];
        arr.push(`${schema}.${tbl}`);
        tableToSchemas.set(tbl, arr);
    }
    for (const [tbl, arr] of tableToSchemas.entries()) {
        if (arr.length === 1) {
            map.set(tbl, arr[0]);
        }
    }

    // Replace occurrences in FROM/JOIN of unqualified names with qualified
    return sql.replace(/\b(FROM|JOIN)\s+("[^"]+"\."[^"]+"|"[^"]+"|[a-zA-Z_][\w]*)(\b)/g, (m, kw, target, tail) => {
        let bare = target;
        // strip quotes
        const unquoted = target.replace(/"/g, '');
        const parts = unquoted.split('.');
        if (parts.length === 1) {
            const tbl = parts[0].toLowerCase();
            const fq = map.get(tbl);
            if (fq) {
                const [schema, name] = fq.split('.');
                return `${kw} "${schema}"."${name}"${tail}`;
            }
        }
        return m;
    });
}

// Singleton PG pool
let pool: Pool | null = null;
function getPool(): Pool {
    if (!pool) {
        if (!PG_CONNECTION_STRING) {
            throw new Error('PG_CONNECTION_STRING (or DATABASE_URL) is required');
        }
        pool = new Pool({ connectionString: PG_CONNECTION_STRING, max: 3 });
    }
    return pool;
}

// Schema loader: read relevant parts of information_schema for public schema
async function loadDatabaseSchema(): Promise<{ schemaText: string; allowedTables: string[] }> {
    const client = await getPool().connect();
    try {
        const tablesRes = await client.query<{
            table_schema: string;
            table_name: string;
            column_name: string;
            data_type: string;
        }>(
            `
      SELECT c.table_schema, c.table_name, c.column_name, c.data_type
      FROM information_schema.columns c
      JOIN information_schema.tables t
        ON c.table_schema = t.table_schema AND c.table_name = t.table_name
      WHERE t.table_type = 'BASE TABLE' AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY c.table_schema, c.table_name, c.ordinal_position;
      `
        );

        // Group columns per table
        const byTable = new Map<string, { schema: string; table: string; columns: string[] }>();
        for (const row of tablesRes.rows) {
            const key = `${row.table_schema}.${row.table_name}`;
            if (!byTable.has(key)) {
                byTable.set(key, { schema: row.table_schema, table: row.table_name, columns: [] });
            }
            byTable.get(key)!.columns.push(`${row.column_name} ${row.data_type}`);
        }

        const lines: string[] = [];
        const allowedTables: string[] = [];
        for (const [key, v] of byTable.entries()) {
            lines.push(`TABLE ${key} ( ${v.columns.join(', ')} )`);
            allowedTables.push(key);
        }
        return { schemaText: lines.join('\n'), allowedTables };
    } finally {
        client.release();
    }
}

// State definition using Annotation API
const Schema = Annotation.Root({
    question: Annotation<string>(),
    schema: Annotation<string>(),
    allowedTables: Annotation<string[]>(),
    sql: Annotation<string>(),
    sqlClean: Annotation<string>(),
    rows: Annotation<any[]>(),
    answer: Annotation<string>(),
    error: Annotation<string>(),
});

// Node: introspect schema
async function nodeIntrospect(state: typeof Schema.State) {
    const { schemaText, allowedTables } = await loadDatabaseSchema();
    return { schema: schemaText, allowedTables };
}

// Basic validation: ensure SQL references only allowed tables (expects fully-qualified names)
function validateSqlTables(sql: string, allowedTables: string[]): { ok: boolean; unknown: string[]; noneFound: boolean; foundTables: string[] } {
    const set = new Set(allowedTables.map(t => t.toLowerCase()));
    const found = new Set<string>();
    // Match schema.table appearances (very simple heuristic)
    const re = /\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(sql)) !== null) {
        found.add(`${m[1]}.${m[2]}`.toLowerCase());
    }
    let unknown = Array.from(found).filter(f => !set.has(f));
    let noneFound = found.size === 0;

    // If none found, try to infer from FROM/JOIN clauses with unqualified names
    if (noneFound) {
        const inferred = new Set<string>();
        // Capture FROM and JOIN targets: FROM "schema"."table"|FROM "table"|JOIN ...
        const fromJoinRe = /\b(FROM|JOIN)\s+(("[^"]+")\.("[^"]+")|("[^"]+")|([a-zA-Z_][\w]*)(?:\.([a-zA-Z_][\w]*))?)/gi;
        let fm: RegExpExecArray | null;
        while ((fm = fromJoinRe.exec(sql)) !== null) {
            const full = fm[2];
            // Strip quotes
            const parts = full.replace(/"/g, '').split('.');
            if (parts.length === 2) {
                inferred.add(`${parts[0]}.${parts[1]}`.toLowerCase());
            } else if (parts.length === 1) {
                const tbl = parts[0].toLowerCase();
                // Try to uniquely map to allowedTables by table name
                const matches = Array.from(set).filter(t => t.endsWith(`.${tbl}`));
                if (matches.length === 1) {
                    inferred.add(matches[0]);
                }
            }
        }
        if (inferred.size > 0) {
            for (const t of inferred) found.add(t);
            unknown = Array.from(found).filter(f => !set.has(f));
            noneFound = found.size === 0;
        }
    }

    return { ok: unknown.length === 0 && !noneFound, unknown, noneFound, foundTables: Array.from(found) };
}

// Node: generate SQL with LLM
async function nodeGenerateSQL(state: typeof Schema.State) {
    const { question, schema, allowedTables } = state;

    const system = `You are a helpful data analyst that writes PostgreSQL SQL queries.
Constraints:
- Use ONLY the tables and columns available in the provided schema.
- Prefer safe, read-only queries (SELECT only).
- If aggregation is requested (e.g., top products), include ORDER BY and LIMIT.
- Use ANSI SQL and functions supported by PostgreSQL.
- Quote identifiers (schema, table, and column names) with double quotes EXACTLY as they appear in the SCHEMA/ALLOWED_TABLES (PostgreSQL is case-sensitive with quoted identifiers).
- Do NOT add WHERE filters unless they are clearly implied by the question.
- Prefer straightforward queries (e.g., ORDER BY + LIMIT for top-N) without unnecessary conditions.
- For single-table queries, fully-qualify and quote the table ONLY in the FROM clause (e.g., FROM "public"."Producto"). In SELECT/ORDER BY, use only quoted column names without table prefixes (e.g., SELECT "nombre" ... ORDER BY "precio" DESC).
- Only use table qualifiers on columns when there are multiple tables and ambiguity.
- Return ONLY the SQL, without explanation or markdown fences.`;

    const prompt = `SCHEMA\n${schema}\n\nALLOWED_TABLES (use only fully qualified names, and quote exactly like this):\n${allowedTables.map(t => `- ${t}`).join('\n')}\n\nQUESTION: ${question}\n\nWrite a single PostgreSQL SQL query to answer the question. You MUST use only tables from ALLOWED_TABLES and you MUST double-quote identifiers exactly as shown (e.g., "public"."Producto"). If the question cannot be answered with these tables, write: SELECT 1 WHERE FALSE;`;

    const llm = new ChatOpenAI({ model: OPENAI_MODEL, temperature: 0 });
    const msg = await llm.invoke([
        { role: 'system', content: system },
        { role: 'user', content: prompt },
    ]);

    let sql = (msg.content as any) as string;
    if (Array.isArray(msg.content)) {
        // Handle content blocks case
        const parts = msg.content.map((c: any) => (typeof c === 'string' ? c : c.text || '')).join('\n');
        sql = parts;
    }

    // Small sanitation: strip code fences/backticks
    sql = sql.trim().replace(/^```[a-zA-Z]*\n?|```$/g, '').trim();
    // Compact to a single line for readability in responses
    sql = sql.replace(/\s+/g, ' ').trim();

    // Try to auto-qualify unqualified tables in FROM/JOIN when unique mapping exists
    sql = qualifySqlTables(sql, allowedTables);

    // Validate tables
    const { ok, unknown, noneFound, foundTables } = validateSqlTables(sql, allowedTables);
    if (!ok) {
        const msgError = noneFound
            ? `SQL must reference at least one fully-qualified table from: ${allowedTables.join(', ')}. Use schema.table names.`
            : `SQL references unknown tables: ${unknown.join(', ')}. Allowed tables: ${allowedTables.join(', ')}`;
        return { sql, answer: msgError, error: 'UNKNOWN_TABLES' };
    }

    // Prepare a cleaned version for display, but keep qualified SQL for execution
    let sqlClean = sql;
    if (foundTables.length === 1) {
        const [schemaName, tableName] = foundTables[0].split('.');
        const qualifierPattern = new RegExp(`"${schemaName}"\."${tableName}"\.`, 'g');
        sqlClean = sqlClean.replace(qualifierPattern, '');
    }

    return { sql, sqlClean };
}

// Node: execute SQL
async function nodeExecuteSQL(state: typeof Schema.State) {
    const { sql, allowedTables } = state;
    // Re-validate before execution
    const { ok, unknown, noneFound, foundTables } = validateSqlTables(sql, allowedTables);
    if (!ok) {
        const msgError = noneFound
            ? `Blocked execution. SQL must reference at least one fully-qualified allowed table.`
            : `Blocked execution. Unknown tables: ${unknown.join(', ')}`;
        return { rows: [], answer: msgError };
    }

    const client = await getPool().connect();
    try {
        const res = await client.query(sql);
        if (Array.isArray(res.rows) && res.rows.length === 0) {
            // If empty result, provide diagnostics: count rows in the first referenced table
            const first = foundTables[0];
            if (first) {
                const [schemaName, tableName] = first.split('.');
                const q = `SELECT COUNT(*)::int AS count FROM "${schemaName}"."${tableName}"`;
                const countRes = await client.query(q);
                const total = countRes.rows?.[0]?.count ?? 0;
                return { rows: [], answer: `No results. Diagnostics: "${schemaName}"."${tableName}" has ${total} rows.` };
            }
        }
        return { rows: res.rows };
    } catch (err: any) {
        const msg = err?.message || 'Database error';
        return { rows: [], answer: `DB error: ${msg}` };
    } finally {
        client.release();
    }
}

// Node: answer formatting with LLM to Markdown (formal, no SQL included)
async function nodeAnswer(state: typeof Schema.State) {
    const { rows, answer: priorAnswer, question, sql, allowedTables } = state as any;

    // If no LLM key, fallback to simple summary
    if (!process.env.OPENAI_API_KEY) {
        let fallback = priorAnswer && priorAnswer.trim().length > 0 ? priorAnswer : '';
        if (!fallback) {
            if (Array.isArray(rows) && rows.length > 0) {
                const firstKeys = Object.keys(rows[0] || {});
                fallback = `Found ${rows.length} rows. Keys: ${firstKeys.join(', ')}. Sample: ${JSON.stringify(rows.slice(0, 3))}`;
            } else {
                fallback = 'No results.';
            }
        }
        return { answer: fallback };
    }

    const rowsSample = Array.isArray(rows) ? rows.slice(0, 10) : [];
    const rowsForStats = Array.isArray(rows) ? rows.slice(0, 100) : [];

    // Helpers for formatting
    const moneyNameHints = ['precio', 'monto', 'total', 'valor', 'costo', 'venta', 'ventas', 'importe', 'subtotal', 'neto', 'bruto'];
    const isMoneyColumn = (name: string) => {
        const n = (name || '').toString().toLowerCase();
        return moneyNameHints.some(h => n.includes(h));
    };
    const clpFmt = new Intl.NumberFormat('es-CL', { style: 'currency', currency: 'CLP', minimumFractionDigits: 0, maximumFractionDigits: 0 });
    const toNumberMaybe = (v: any): number | null => {
        if (typeof v === 'number' && Number.isFinite(v)) return v;
        if (typeof v === 'string') {
            const s = v.replace(/\s/g, '').replace(/\./g, '').replace(/,/g, '.');
            const n = Number(s);
            return Number.isFinite(n) ? n : null;
        }
        return null;
    };
    const formatCLP = (num: number) => {
        if (typeof num !== 'number' || !Number.isFinite(num)) return '';
        // Round to nearest peso
        const rounded = Math.round(num);
        if (rounded < 0) return `-${clpFmt.format(Math.abs(rounded))}`;
        return clpFmt.format(rounded);
    };
    const escapeHtml = (str: string) => str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    const escapeCell = (v: any) => {
        if (v === null || v === undefined) return '';
        let s = typeof v === 'string' ? v : (typeof v === 'number' ? v.toString() : JSON.stringify(v));
        s = s.replace(/\n|\r/g, ' ');
        return escapeHtml(s);
    };

    // LLM will generate the HTML results table with Tailwind; backend no longer builds it.

    // Detect numeric columns and compute simple stats
    const numericStats: Record<string, { count: number; min: number; max: number; avg: number }> = {};
    if (rowsForStats.length > 0) {
        const keys = Object.keys(rowsForStats[0] || {});
        for (const k of keys) {
            const nums: number[] = [];
            for (const r of rowsForStats) {
                const v = (r as any)[k];
                const n = toNumberMaybe(v);
                if (n !== null) nums.push(n);
            }
            if (nums.length > 0) {
                const sum = nums.reduce((a, b) => a + b, 0);
                const min = Math.min(...nums);
                const max = Math.max(...nums);
                const avg = sum / nums.length;
                numericStats[k] = { count: nums.length, min, max, avg };
            }
        }
    }

    // Create a CLP-formatted view of stats for money-like columns
    const numericStatsFormatted: Record<string, any> = {};
    for (const [k, v] of Object.entries(numericStats)) {
        if (isMoneyColumn(k)) {
            numericStatsFormatted[k] = {
                count: v.count,
                min: formatCLP(v.min),
                max: formatCLP(v.max),
                avg: formatCLP(v.avg),
            };
        } else {
            numericStatsFormatted[k] = v;
        }
    }

    // Detect referenced tables (best-effort) for context
    const { foundTables } = validateSqlTables((sql ?? '').toString(), Array.isArray(allowedTables) ? allowedTables : []);

    const system = `Eres un analista de datos senior. Respondes en Español con tono profesional. Genera Markdown y HTML (rehype-raw). Estructura SIEMPRE la salida en secciones con encabezados: \n- Resumen ejecutivo (breve, accionable)\n- Hallazgos clave (bullets)\n- Resultados (tabla HTML con Tailwind)\n- Análisis (tendencias, outliers, comparaciones)\n- Recomendaciones (pasos accionables)\n- Limitaciones (calidad de datos, supuestos)\n- Próximos pasos.\n\nTabla de Resultados (OBLIGATORIO):\n- Genera una tabla HTML accesible con Tailwind:\n  <div class="overflow-x-auto rounded-md ring-1 ring-gray-200 dark:ring-gray-700">\n    <caption class="block text-left text-sm text-gray-500 dark:text-gray-400 p-3">Resultados</caption>\n    <table class="min-w-full w-full table-fixed text-sm text-gray-700 dark:text-gray-200">\n      <thead class="bg-gray-50 dark:bg-gray-800">\n        <tr class="border-b border-gray-200 dark:border-gray-700">... th ...</tr>\n      </thead>\n      <tbody>... tr/td ...</tbody>\n    </table>\n  </div>\n- Alinea a la derecha columnas monetarias (text-right, tabular-nums).\n- Zebra striping en filas: odd:bg-white even:bg-gray-50 dark:odd:bg-gray-900 dark:even:bg-gray-800, y hover.\n\nReglas de formato de montos (CLP):\n- Formatea como $1.234.567 (sin decimales); negativos como -$1.234.567.\n- Si el valor viene como string (con símbolos, espacios, puntos o comas), normalízalo sin alterar la magnitud:\n  ejemplos: "CLP 1,234.56" -> $1.235; "1.234,56" -> $1.235; "$ 12 345" -> $12.345.\n- NO inventes cifras ni cambies cantidades; solo cambia la representación (separadores/decimales).\n\nNO incluyas la consulta SQL. Evita jerga innecesaria.`;
    const user = `PREGUNTA: ${question}\nTABLAS REFERENCIADAS: ${foundTables.join(', ') || 'N/D'}\nFILAS (JSON) - muestra hasta 10 (sin formateo, pueden venir strings numéricas):\n${JSON.stringify(rowsSample)}\nESTADÍSTICAS NUMÉRICAS (sobre muestra hasta 100) — usa estos valores VERBATIM si los mencionas:\n${JSON.stringify(numericStatsFormatted)}\n${priorAnswer && priorAnswer.trim().length > 0 ? `\nNOTA (diagnóstico): ${priorAnswer}\n` : ''}\nInstrucciones: Genera la tabla HTML completa en la sección Resultados aplicando las reglas CLP y Tailwind indicadas. No alteres las cantidades.`;

    const llm = new ChatOpenAI({ model: OPENAI_MODEL, temperature: 0 });
    const msg = await llm.invoke([
        { role: 'system', content: system },
        { role: 'user', content: user },
    ]);
    let md = (msg.content as any) as string;
    if (Array.isArray(msg.content)) {
        md = msg.content.map((c: any) => (typeof c === 'string' ? c : c.text || '')).join('\n');
    }
    const answer = (md?.toString() || '').trim() || priorAnswer || 'Sin resultados.';
    return { answer };
}

// Build graph
const graph = new StateGraph(Schema)
    .addNode('introspect', nodeIntrospect)
    .addNode('generate_sql', nodeGenerateSQL)
    .addNode('execute_sql', nodeExecuteSQL)
    .addNode('format_answer', nodeAnswer)
    .addEdge('__start__', 'introspect')
    .addEdge('introspect', 'generate_sql')
    .addEdge('generate_sql', 'execute_sql')
    .addEdge('execute_sql', 'format_answer')
    .addEdge('format_answer', '__end__')
    .compile();

export async function runSqlAgent(question: string) {
    const result = await graph.invoke({ question });
    return result as typeof Schema.State;
}
