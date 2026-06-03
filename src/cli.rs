use anyhow::{Context, ensure};
use base64::Engine;
use serde_json::{Value, json};
use std::io::{self, Write};

const DEFAULT_URL: &str = "http://127.0.0.1:8091/appdb";

pub async fn run_cli(url: Option<String>) -> anyhow::Result<()> {
    let raw_url = default_cli_url(url)?;
    let endpoint = PipelineEndpoint::parse(&raw_url)?;
    let auth_token = std::env::var("LIBSQL_AUTH_TOKEN").ok();
    let mut shell = OrionCli {
        client: reqwest::Client::new(),
        endpoint,
        auth_token,
        baton: None,
        buffer: String::new(),
    };
    shell.run().await
}

fn default_cli_url(url: Option<String>) -> anyhow::Result<String> {
    if let Some(url) = url {
        return Ok(url);
    }
    if let Ok(url) = std::env::var("LIBSQL_URL") {
        return Ok(url);
    }
    if let Ok(url) = std::env::var("ORION_URL") {
        return database_url_from_orion_url(&url, "appdb");
    }
    Ok(DEFAULT_URL.to_string())
}

fn database_url_from_orion_url(raw_url: &str, database: &str) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(raw_url)
        .with_context(|| format!("parsing ORION_URL endpoint {raw_url:?}"))?;
    let path = url.path().trim_matches('/');
    if path.is_empty() || path == "_orion" {
        url.set_path(&format!("/{database}"));
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

struct OrionCli {
    client: reqwest::Client,
    endpoint: PipelineEndpoint,
    auth_token: Option<String>,
    baton: Option<String>,
    buffer: String,
}

impl OrionCli {
    async fn run(&mut self) -> anyhow::Result<()> {
        println!("Connected to {}", self.endpoint.display_url);
        println!("Enter SQL terminated by ';'. Use .help for shell commands.");

        loop {
            let prompt = if self.buffer.is_empty() {
                "orion> "
            } else {
                "   ...> "
            };
            let Some(line) = read_line(prompt)? else {
                break;
            };
            let trimmed = line.trim();

            if self.buffer.is_empty() && trimmed.starts_with('.') {
                if !self.run_dot_command(trimmed).await? {
                    break;
                }
                continue;
            }

            if self.buffer.is_empty() {
                self.buffer = line;
            } else {
                self.buffer.push('\n');
                self.buffer.push_str(&line);
            }

            if !statement_is_complete(&self.buffer) {
                continue;
            }

            let sql = strip_trailing_semicolon(self.buffer.trim()).to_string();
            self.buffer.clear();
            if sql.is_empty() {
                continue;
            }

            if let Err(error) = self.run_sql(&sql, Vec::new()).await {
                eprintln!("ERROR: {error}");
            }
        }

        self.close_session().await.ok();
        Ok(())
    }

    async fn run_dot_command(&mut self, command: &str) -> anyhow::Result<bool> {
        match command {
            ".exit" | ".quit" | ".q" => Ok(false),
            ".help" => {
                println!(
                    "{}",
                    [
                        "Shell commands:",
                        "  .help              Show this help",
                        "  .tables            List tables",
                        "  .databases         Show the current database URL",
                        "  .system            List Orion system tables",
                        "  .metrics           Show live Raft metrics",
                        "  .schema [table]    Show CREATE statements",
                        "  .quit              Exit",
                        "",
                        "Environment:",
                        "  LIBSQL_URL         Default database URL",
                        "  LIBSQL_AUTH_TOKEN  Bearer token for authenticated endpoints",
                    ]
                    .join("\n")
                );
                Ok(true)
            }
            ".tables" => {
                if self.endpoint.database == "_orion" {
                    self.run_sql("select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' union all select 'raft_metrics' union all select 'storage_pressure' order by name", Vec::new()).await?;
                } else {
                    self.run_sql(
                        "select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' order by name",
                        Vec::new(),
                    )
                    .await?;
                }
                Ok(true)
            }
            ".databases" => {
                println!("current: {}", self.endpoint.display_url);
                println!("system:  {}", self.endpoint.system_url);
                Ok(true)
            }
            ".system" => {
                println!("System namespace: _orion");
                println!("Tables:");
                println!("  compaction_runs");
                println!("  compaction_state");
                println!("  database_catalog");
                println!("  database_placement");
                println!("  database_standbys");
                println!("  placement_metrics");
                println!("  placement_nodes");
                println!("  raft_metrics       (virtual, live)");
                println!("  storage_pressure   (virtual, live)");
                Ok(true)
            }
            ".metrics" => {
                self.run_sql("select * from raft_metrics", Vec::new())
                    .await?;
                Ok(true)
            }
            command if command == ".schema" || command.starts_with(".schema ") => {
                let table = command.trim_start_matches(".schema").trim();
                if table.is_empty() {
                    self.run_sql(
                        "select sql from sqlite_schema where sql is not null order by type, name",
                        Vec::new(),
                    )
                    .await?;
                } else {
                    self.run_sql(
                        "select sql from sqlite_schema where sql is not null and name = ? order by type, name",
                        vec![json!({ "type": "text", "value": table })],
                    )
                    .await?;
                }
                Ok(true)
            }
            _ => {
                eprintln!("Unknown command: {command}");
                Ok(true)
            }
        }
    }

    async fn run_sql(&mut self, sql: &str, args: Vec<Value>) -> anyhow::Result<()> {
        let response = self
            .pipeline(
                vec![json!({
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "args": args,
                        "want_rows": statement_can_return_rows(sql),
                    }
                })],
                true,
            )
            .await?;

        let result = response
            .get("results")
            .and_then(Value::as_array)
            .and_then(|results| results.first())
            .context("libSQL response missing first result")?;
        if result.get("type").and_then(Value::as_str) == Some("error") {
            let message = result
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("unknown libSQL error");
            anyhow::bail!("{message}");
        }

        let stmt_result = result
            .get("response")
            .and_then(|response| response.get("result"))
            .context("libSQL response missing statement result")?;
        print_statement_result(stmt_result);
        Ok(())
    }

    async fn close_session(&mut self) -> anyhow::Result<()> {
        if self.baton.is_none() {
            return Ok(());
        }
        self.pipeline(vec![json!({ "type": "close" })], false)
            .await?;
        Ok(())
    }

    async fn pipeline(&mut self, requests: Vec<Value>, keep_baton: bool) -> anyhow::Result<Value> {
        let mut body = json!({ "requests": requests });
        if let Some(baton) = &self.baton {
            body["baton"] = json!(baton);
        }

        let mut request = self
            .client
            .post(self.endpoint.pipeline_url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body);
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }

        let response = request.send().await.context("sending libSQL pipeline")?;
        let status = response.status();
        ensure!(status.is_success(), "HTTP {} {}", status.as_u16(), status);

        let payload = response
            .json::<Value>()
            .await
            .context("decoding libSQL response JSON")?;
        if keep_baton {
            if let Some(baton) = payload.get("baton").and_then(Value::as_str) {
                self.baton = Some(baton.to_string());
            }
        } else {
            self.baton = None;
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone)]
struct PipelineEndpoint {
    display_url: String,
    pipeline_url: String,
    database: String,
    system_url: String,
}

impl PipelineEndpoint {
    fn parse(raw_url: &str) -> anyhow::Result<Self> {
        let mut url = reqwest::Url::parse(raw_url)
            .with_context(|| format!("parsing libSQL endpoint URL {raw_url:?}"))?;
        let mut pathname = url.path().trim_end_matches('/').to_string();
        if !pathname.ends_with("/v2/pipeline") {
            pathname = format!("{}/v2/pipeline", pathname);
        }
        url.set_path(&pathname);
        let pipeline_url = url.to_string();
        let database = database_name_from_path(url.path());
        let mut system_url = url;
        system_url.set_path("/_orion");

        Ok(Self {
            display_url: raw_url.to_string(),
            pipeline_url,
            database,
            system_url: system_url.to_string(),
        })
    }
}

fn read_line(prompt: &str) -> anyhow::Result<Option<String>> {
    print!("{prompt}");
    io::stdout().flush().context("flushing prompt")?;
    let mut line = String::new();
    let bytes = io::stdin().read_line(&mut line).context("reading stdin")?;
    if bytes == 0 {
        return Ok(None);
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(Some(line))
}

fn print_statement_result(result: &Value) {
    let columns: Vec<String> = result
        .get("cols")
        .and_then(Value::as_array)
        .map(|cols| {
            cols.iter()
                .enumerate()
                .map(|(index, col)| {
                    col.get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("column{}", index + 1))
                })
                .collect()
        })
        .unwrap_or_default();
    let rows: Vec<Vec<String>> = result
        .get("rows")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(Value::as_array)
                .map(|row| row.iter().map(render_hrana_value).collect())
                .collect()
        })
        .unwrap_or_default();

    if !columns.is_empty() {
        print_table(&columns, &rows);
        println!(
            "({} {})",
            rows.len(),
            if rows.len() == 1 { "row" } else { "rows" }
        );
        return;
    }

    let affected = result
        .get("affected_row_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    println!(
        "OK ({} {} affected)",
        affected,
        if affected == 1 { "row" } else { "rows" }
    );
}

fn print_table(columns: &[String], rows: &[Vec<String>]) {
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|value| value.len())
                .max()
                .unwrap_or(0)
                .max(column.len())
        })
        .collect();
    println!("{}", format_row(columns, &widths));
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-")
    );
    for row in rows {
        println!("{}", format_row(row, &widths));
    }
}

fn format_row(row: &[String], widths: &[usize]) -> String {
    row.iter()
        .enumerate()
        .map(|(index, value)| format!("{value:<width$}", width = widths[index]))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn render_hrana_value(value: &Value) -> String {
    match value.get("type").and_then(Value::as_str) {
        Some("null") => "NULL".to_string(),
        Some("integer") | Some("text") => value
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        Some("float") => value
            .get("value")
            .map(|value| {
                value
                    .as_f64()
                    .map(|float| float.to_string())
                    .unwrap_or_else(|| value.to_string())
            })
            .unwrap_or_default(),
        Some("blob") => {
            let bytes = value
                .get("base64")
                .and_then(Value::as_str)
                .and_then(|encoded| {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()
                })
                .unwrap_or_default();
            format!("x'{}'", hex_lower(&bytes))
        }
        _ => value.to_string(),
    }
}

fn statement_can_return_rows(sql: &str) -> bool {
    let keyword = sql
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_lowercase();
    matches!(
        keyword.as_str(),
        "select" | "with" | "pragma" | "explain" | "values"
    )
}

fn statement_is_complete(sql: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    for ch in sql.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(current_quote) = quote {
            if ch == current_quote {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '\'' | '"' | '`') {
            quote = Some(ch);
        }
    }
    quote.is_none() && sql.trim_end().ends_with(';')
}

fn strip_trailing_semicolon(sql: &str) -> &str {
    sql.trim_end().strip_suffix(';').unwrap_or(sql).trim_end()
}

fn database_name_from_path(pathname: &str) -> String {
    let parts: Vec<&str> = pathname
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    parts
        .iter()
        .position(|part| *part == "v2")
        .and_then(|v2_index| v2_index.checked_sub(1))
        .and_then(|database_index| parts.get(database_index))
        .or_else(|| parts.first())
        .copied()
        .unwrap_or("orion")
        .to_string()
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
