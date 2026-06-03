use anyhow::{Context, ensure};
use serde_json::{Value, json};

const DEFAULT_ORION_URL: &str = "http://127.0.0.1:8091";

pub async fn run_db_cli(args: Vec<String>) -> anyhow::Result<()> {
    let parsed = DbCliArgs::parse(args)?;
    let client = DbClient::new(parsed.orion_url)?;
    match parsed.command {
        DbCommand::List { include_deleted } => {
            let query = include_deleted.then_some("include_deleted=true");
            print_json(client.get("/_orion/databases", query).await?)
        }
        DbCommand::Create {
            name,
            idempotency_key,
        } => print_json(
            client
                .post(
                    "/_orion/databases",
                    None,
                    json!({ "name": name }),
                    idempotency_key.as_deref(),
                )
                .await?,
        ),
        DbCommand::Get { name } => print_json(
            client
                .get(&format!("/_orion/databases/{name}"), None)
                .await?,
        ),
        DbCommand::Drop {
            name,
            idempotency_key,
        } => print_json(
            client
                .delete(
                    &format!("/_orion/databases/{name}"),
                    None,
                    idempotency_key.as_deref(),
                )
                .await?,
        ),
        DbCommand::Placement { name } => print_json(
            client
                .get(&format!("/_orion/databases/{name}/placement"), None)
                .await?,
        ),
        DbCommand::Explain { name } => {
            print_json(explain_database_placement(&client, &name).await?)
        }
        DbCommand::Plan { name, target_group } => print_json(
            client
                .post(
                    &format!("/_orion/databases/{name}/placement/plan"),
                    None,
                    json!({ "target_group_id": target_group }),
                    None,
                )
                .await?,
        ),
        DbCommand::Move {
            name,
            target_group,
            drain_timeout_ms,
        } => {
            let mut body = json!({ "target_group_id": target_group });
            if let Some(drain_timeout_ms) = drain_timeout_ms {
                body["drain_timeout_ms"] = json!(drain_timeout_ms);
            }
            print_json(
                client
                    .post(
                        &format!("/_orion/databases/{name}/placement/move"),
                        None,
                        body,
                        None,
                    )
                    .await?,
            )
        }
        DbCommand::Standbys { name } => print_json(
            client
                .get(
                    &format!("/_orion/databases/{name}/placement/standbys"),
                    None,
                )
                .await?,
        ),
        DbCommand::RefreshStandby { name, target_group } => print_json(
            client
                .post(
                    &format!("/_orion/databases/{name}/placement/standby"),
                    None,
                    json!({ "target_group_id": target_group }),
                    None,
                )
                .await?,
        ),
        DbCommand::Promote {
            name,
            target_group,
            max_staleness_ms,
            force,
        } => {
            let mut body = json!({
                "target_group_id": target_group,
                "force": force,
            });
            if let Some(max_staleness_ms) = max_staleness_ms {
                body["max_staleness_ms"] = json!(max_staleness_ms);
            }
            print_json(
                client
                    .post(
                        &format!("/_orion/databases/{name}/placement/promote"),
                        None,
                        body,
                        None,
                    )
                    .await?,
            )
        }
        DbCommand::Operations { name } => print_json(
            client
                .get(
                    &format!("/_orion/databases/{name}/placement/operations"),
                    None,
                )
                .await?,
        ),
        DbCommand::Cancel {
            name,
            operation_id,
            reason,
        } => print_json(
            client
                .post(
                    &format!("/_orion/databases/{name}/placement/operations/{operation_id}/cancel"),
                    None,
                    json!({ "reason": reason }),
                    None,
                )
                .await?,
        ),
        DbCommand::Repair {
            name,
            operation_id,
            phase,
            reason,
        } => {
            let mut body = json!({});
            if let Some(phase) = phase {
                body["phase"] = json!(phase);
            }
            if let Some(reason) = reason {
                body["reason"] = json!(reason);
            }
            print_json(
                client
                    .post(
                        &format!(
                            "/_orion/databases/{name}/placement/operations/{operation_id}/repair"
                        ),
                        None,
                        body,
                        None,
                    )
                    .await?,
            )
        }
        DbCommand::Groups(command) => run_group_command(&client, command).await,
        DbCommand::Reconcile => print_json(
            client
                .post("/_orion/placement/reconcile", None, json!({}), None)
                .await?,
        ),
        DbCommand::StandbyReconcile => print_json(
            client
                .post("/_orion/placement/standby/reconcile", None, json!({}), None)
                .await?,
        ),
        DbCommand::Metrics => print_json(client.get("/_orion/metrics/placement", None).await?),
    }
}

struct DbCliArgs {
    orion_url: String,
    command: DbCommand,
}

impl DbCliArgs {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut orion_url = None;
        let mut rest = Vec::new();
        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--orion-url" | "-u" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} requires a URL"))?;
                    orion_url = Some(value);
                }
                "--help" | "-h" => return Err(anyhow::anyhow!(DB_USAGE)),
                _ => rest.push(arg),
            }
        }
        let orion_url = orion_url
            .or_else(|| std::env::var("ORION_URL").ok())
            .unwrap_or_else(|| DEFAULT_ORION_URL.to_string());
        let command = DbCommand::parse(rest)?;
        Ok(Self { orion_url, command })
    }
}

enum DbCommand {
    List {
        include_deleted: bool,
    },
    Create {
        name: String,
        idempotency_key: Option<String>,
    },
    Get {
        name: String,
    },
    Drop {
        name: String,
        idempotency_key: Option<String>,
    },
    Placement {
        name: String,
    },
    Explain {
        name: String,
    },
    Plan {
        name: String,
        target_group: String,
    },
    Move {
        name: String,
        target_group: String,
        drain_timeout_ms: Option<u64>,
    },
    Standbys {
        name: String,
    },
    RefreshStandby {
        name: String,
        target_group: String,
    },
    Promote {
        name: String,
        target_group: String,
        max_staleness_ms: Option<u64>,
        force: bool,
    },
    Operations {
        name: String,
    },
    Cancel {
        name: String,
        operation_id: String,
        reason: Option<String>,
    },
    Repair {
        name: String,
        operation_id: String,
        phase: Option<String>,
        reason: Option<String>,
    },
    Groups(GroupCommand),
    Reconcile,
    StandbyReconcile,
    Metrics,
}

enum GroupCommand {
    List,
    Runtime,
    Get {
        group_id: String,
    },
    Create {
        group_id: String,
        mode: String,
        automatic_failover: bool,
        promote_after_ms: u64,
        standby_targets: Vec<String>,
        members: Vec<String>,
    },
    AddMember {
        group_id: String,
        node_id: u64,
        role: String,
        priority: Option<u64>,
    },
    RemoveMember {
        group_id: String,
        node_id: u64,
        role: String,
    },
    Drain {
        group_id: String,
    },
    Delete {
        group_id: String,
    },
}

impl DbCommand {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut args = ArgCursor::new(args);
        let command = args.next_required(DB_USAGE)?;
        let parsed = match command.as_str() {
            "list" | "ls" => {
                let include_deleted = args.take_flag("--include-deleted");
                args.ensure_empty(DB_USAGE)?;
                Self::List { include_deleted }
            }
            "create" => {
                let name =
                    args.next_required("usage: orion db create <name> [--idempotency-key <key>]")?;
                let idempotency_key = args.take_option("--idempotency-key")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Create {
                    name,
                    idempotency_key,
                }
            }
            "get" | "show" => {
                let name = args.next_required("usage: orion db get <name>")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Get { name }
            }
            "drop" | "delete" | "rm" => {
                let name =
                    args.next_required("usage: orion db drop <name> [--idempotency-key <key>]")?;
                let idempotency_key = args.take_option("--idempotency-key")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Drop {
                    name,
                    idempotency_key,
                }
            }
            "placement" => {
                let name = args.next_required("usage: orion db placement <name>")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Placement { name }
            }
            "explain" => {
                let name = args.next_required("usage: orion db explain <name>")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Explain { name }
            }
            "plan" => {
                let name =
                    args.next_required("usage: orion db plan <name> --target-group <group>")?;
                let target_group =
                    take_target_group_option(&mut args, "orion db plan requires --target-group")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Plan { name, target_group }
            }
            "move" => {
                let name = args.next_required(
                    "usage: orion db move <name> --target-group <group> [--drain-timeout-ms <ms>]",
                )?;
                let target_group =
                    take_target_group_option(&mut args, "orion db move requires --target-group")?;
                let drain_timeout_ms = args
                    .take_option("--drain-timeout-ms")?
                    .map(|value| parse_u64(&value, "--drain-timeout-ms"))
                    .transpose()?;
                args.ensure_empty(DB_USAGE)?;
                Self::Move {
                    name,
                    target_group,
                    drain_timeout_ms,
                }
            }
            "standbys" => {
                let name = args.next_required("usage: orion db standbys <name>")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Standbys { name }
            }
            "refresh-standby" => {
                let name = args.next_required(
                    "usage: orion db refresh-standby <name> --target-group <group>",
                )?;
                let target_group = take_target_group_option(
                    &mut args,
                    "orion db refresh-standby requires --target-group",
                )?;
                args.ensure_empty(DB_USAGE)?;
                Self::RefreshStandby { name, target_group }
            }
            "promote" => {
                let name = args.next_required(
                    "usage: orion db promote <name> --target-group <group> [--max-staleness-ms <ms>] [--force]",
                )?;
                let target_group = take_target_group_option(
                    &mut args,
                    "orion db promote requires --target-group",
                )?;
                let max_staleness_ms = args
                    .take_option("--max-staleness-ms")?
                    .map(|value| parse_u64(&value, "--max-staleness-ms"))
                    .transpose()?;
                let force = args.take_flag("--force");
                args.ensure_empty(DB_USAGE)?;
                Self::Promote {
                    name,
                    target_group,
                    max_staleness_ms,
                    force,
                }
            }
            "operations" | "ops" => {
                let name = args.next_required("usage: orion db operations <name>")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Operations { name }
            }
            "cancel" => {
                let name = args.next_required(
                    "usage: orion db cancel <name> <operation-id> [--reason <reason>]",
                )?;
                let operation_id = args.next_required(
                    "usage: orion db cancel <name> <operation-id> [--reason <reason>]",
                )?;
                let reason = args.take_option("--reason")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Cancel {
                    name,
                    operation_id,
                    reason,
                }
            }
            "repair" => {
                let name = args.next_required(
                    "usage: orion db repair <name> <operation-id> [--phase <phase>] [--reason <reason>]",
                )?;
                let operation_id = args.next_required(
                    "usage: orion db repair <name> <operation-id> [--phase <phase>] [--reason <reason>]",
                )?;
                let phase = args.take_option("--phase")?;
                let reason = args.take_option("--reason")?;
                args.ensure_empty(DB_USAGE)?;
                Self::Repair {
                    name,
                    operation_id,
                    phase,
                    reason,
                }
            }
            "groups" | "replication-groups" | "rg" => Self::Groups(GroupCommand::parse(args)?),
            "reconcile" => {
                args.ensure_empty(DB_USAGE)?;
                Self::Reconcile
            }
            "standby-reconcile" => {
                args.ensure_empty(DB_USAGE)?;
                Self::StandbyReconcile
            }
            "metrics" => {
                args.ensure_empty(DB_USAGE)?;
                Self::Metrics
            }
            "--help" | "-h" | "help" => return Err(anyhow::anyhow!(DB_USAGE)),
            _ => {
                return Err(anyhow::anyhow!(
                    "unknown db command {command:?}\n\n{DB_USAGE}"
                ));
            }
        };
        Ok(parsed)
    }
}

impl GroupCommand {
    fn parse(mut args: ArgCursor) -> anyhow::Result<Self> {
        let command = args.next_required(GROUP_USAGE)?;
        let parsed = match command.as_str() {
            "list" | "ls" => {
                args.ensure_empty(GROUP_USAGE)?;
                Self::List
            }
            "runtime" | "health" => {
                args.ensure_empty(GROUP_USAGE)?;
                Self::Runtime
            }
            "get" | "show" => {
                let group_id = args.next_required("usage: orion db groups get <group-id>")?;
                args.ensure_empty(GROUP_USAGE)?;
                Self::Get { group_id }
            }
            "create" => {
                let group_id = args.next_required(GROUP_CREATE_USAGE)?;
                let mode = args
                    .take_option("--mode")?
                    .unwrap_or_else(|| "manual".to_string());
                let automatic_failover = !args.take_flag("--no-automatic-failover");
                let promote_after_ms = args
                    .take_option("--promote-after-ms")?
                    .map(|value| parse_u64(&value, "--promote-after-ms"))
                    .transpose()?
                    .unwrap_or(600_000);
                let standby_targets = args
                    .take_option("--standby-targets")?
                    .map(|value| split_csv(&value))
                    .unwrap_or_default();
                let mut members = Vec::new();
                while let Some(member) = args.take_option("--member")? {
                    members.push(member);
                }
                args.ensure_empty(GROUP_CREATE_USAGE)?;
                Self::Create {
                    group_id,
                    mode,
                    automatic_failover,
                    promote_after_ms,
                    standby_targets,
                    members,
                }
            }
            "add-member" => {
                let group_id = args.next_required(GROUP_ADD_MEMBER_USAGE)?;
                let node_id = args
                    .next_required(GROUP_ADD_MEMBER_USAGE)?
                    .parse::<u64>()
                    .context("parsing node id")?;
                let role = args
                    .take_option("--role")?
                    .unwrap_or_else(|| "voter".to_string());
                let priority = args
                    .take_option("--priority")?
                    .map(|value| parse_u64(&value, "--priority"))
                    .transpose()?;
                args.ensure_empty(GROUP_ADD_MEMBER_USAGE)?;
                Self::AddMember {
                    group_id,
                    node_id,
                    role,
                    priority,
                }
            }
            "remove-member" | "rm-member" => {
                let group_id = args.next_required(GROUP_REMOVE_MEMBER_USAGE)?;
                let node_id = args
                    .next_required(GROUP_REMOVE_MEMBER_USAGE)?
                    .parse::<u64>()
                    .context("parsing node id")?;
                let role = args
                    .take_option("--role")?
                    .unwrap_or_else(|| "voter".to_string());
                args.ensure_empty(GROUP_REMOVE_MEMBER_USAGE)?;
                Self::RemoveMember {
                    group_id,
                    node_id,
                    role,
                }
            }
            "drain" => {
                let group_id = args.next_required("usage: orion db groups drain <group-id>")?;
                args.ensure_empty(GROUP_USAGE)?;
                Self::Drain { group_id }
            }
            "delete" | "drop" | "rm" => {
                let group_id = args.next_required("usage: orion db groups delete <group-id>")?;
                args.ensure_empty(GROUP_USAGE)?;
                Self::Delete { group_id }
            }
            "--help" | "-h" | "help" => return Err(anyhow::anyhow!(GROUP_USAGE)),
            _ => {
                return Err(anyhow::anyhow!(
                    "unknown groups command {command:?}\n\n{GROUP_USAGE}"
                ));
            }
        };
        Ok(parsed)
    }
}

struct ArgCursor {
    args: Vec<String>,
}

impl ArgCursor {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn next_required(&mut self, usage: &str) -> anyhow::Result<String> {
        if self.args.is_empty() {
            anyhow::bail!("{usage}");
        }
        Ok(self.args.remove(0))
    }

    fn take_flag(&mut self, flag: &str) -> bool {
        if let Some(index) = self.args.iter().position(|arg| arg == flag) {
            self.args.remove(index);
            true
        } else {
            false
        }
    }

    fn take_option(&mut self, flag: &str) -> anyhow::Result<Option<String>> {
        let Some(index) = self.args.iter().position(|arg| arg == flag) else {
            return Ok(None);
        };
        self.args.remove(index);
        if index >= self.args.len() {
            anyhow::bail!("{flag} requires a value");
        }
        Ok(Some(self.args.remove(index)))
    }

    fn ensure_empty(&self, usage: &str) -> anyhow::Result<()> {
        ensure!(
            self.args.is_empty(),
            "unexpected argument {:?}\n\n{usage}",
            self.args[0]
        );
        Ok(())
    }
}

struct DbClient {
    client: reqwest::Client,
    base_url: reqwest::Url,
    auth_token: Option<String>,
}

impl DbClient {
    fn new(raw_url: String) -> anyhow::Result<Self> {
        let mut base_url = reqwest::Url::parse(&raw_url)
            .with_context(|| format!("parsing Orion URL {raw_url:?}"))?;
        base_url.set_path("/");
        base_url.set_query(None);
        base_url.set_fragment(None);
        let auth_token = std::env::var("ORION_AUTH_TOKEN")
            .ok()
            .or_else(|| std::env::var("LIBSQL_AUTH_TOKEN").ok());
        Ok(Self {
            client: reqwest::Client::new(),
            base_url,
            auth_token,
        })
    }

    async fn get(&self, path: &str, query: Option<&str>) -> anyhow::Result<Value> {
        let url = self.url(path, query)?;
        self.send(self.client.get(url), None).await
    }

    async fn post(
        &self,
        path: &str,
        query: Option<&str>,
        body: Value,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Value> {
        let url = self.url(path, query)?;
        self.send(self.client.post(url).json(&body), idempotency_key)
            .await
    }

    async fn delete(
        &self,
        path: &str,
        query: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Value> {
        let url = self.url(path, query)?;
        self.send(self.client.delete(url), idempotency_key).await
    }

    async fn send(
        &self,
        mut request: reqwest::RequestBuilder,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<Value> {
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }
        if let Some(idempotency_key) = idempotency_key {
            request = request.header("x-orion-idempotency-key", idempotency_key);
        }
        let response = request.send().await.context("sending Orion request")?;
        let status = response.status();
        let body = response.text().await.context("reading Orion response")?;
        let json = if body.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str::<Value>(&body)
                .with_context(|| format!("decoding Orion response JSON: {body}"))?
        };
        if !status.is_success() {
            let message = json
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or(body.as_str());
            anyhow::bail!("HTTP {}: {message}", status.as_u16());
        }
        Ok(json)
    }

    fn url(&self, path: &str, query: Option<&str>) -> anyhow::Result<reqwest::Url> {
        let mut url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .with_context(|| format!("building URL for {path}"))?;
        if let Some(query) = query {
            url.set_query(Some(query));
        }
        Ok(url)
    }
}

fn parse_u64(value: &str, flag: &str) -> anyhow::Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("parsing {flag} value {value:?}"))
}

fn take_target_group_option(args: &mut ArgCursor, message: &str) -> anyhow::Result<String> {
    args.take_option("--target-group")?
        .or(args.take_option("-g")?)
        .ok_or_else(|| anyhow::anyhow!(message.to_string()))
}

async fn run_group_command(client: &DbClient, command: GroupCommand) -> anyhow::Result<()> {
    match command {
        GroupCommand::List => print_json(client.get("/_orion/replication-groups", None).await?),
        GroupCommand::Runtime => print_json(
            client
                .get("/_orion/replication-groups/runtime", None)
                .await?,
        ),
        GroupCommand::Get { group_id } => print_json(
            client
                .get(&format!("/_orion/replication-groups/{group_id}"), None)
                .await?,
        ),
        GroupCommand::Create {
            group_id,
            mode,
            automatic_failover,
            promote_after_ms,
            standby_targets,
            members,
        } => {
            let members = members
                .iter()
                .map(|member| parse_group_member(member))
                .collect::<anyhow::Result<Vec<_>>>()?;
            print_json(
                client
                    .post(
                        "/_orion/replication-groups",
                        None,
                        json!({
                            "group_id": group_id,
                            "placement": {
                                "mode": mode,
                                "failover": {
                                    "automatic": automatic_failover,
                                    "promote_after_ms": promote_after_ms,
                                    "standby_targets": standby_targets,
                                }
                            },
                            "members": members,
                        }),
                        None,
                    )
                    .await?,
            )
        }
        GroupCommand::AddMember {
            group_id,
            node_id,
            role,
            priority,
        } => {
            let mut body = json!({
                "node_id": node_id,
                "role": role,
            });
            if let Some(priority) = priority {
                body["priority"] = json!(priority);
            }
            print_json(
                client
                    .post(
                        &format!("/_orion/replication-groups/{group_id}/members"),
                        None,
                        body,
                        None,
                    )
                    .await?,
            )
        }
        GroupCommand::RemoveMember {
            group_id,
            node_id,
            role,
        } => print_json(
            client
                .delete(
                    &format!("/_orion/replication-groups/{group_id}/members/{node_id}/{role}"),
                    None,
                    None,
                )
                .await?,
        ),
        GroupCommand::Drain { group_id } => print_json(
            client
                .post(
                    &format!("/_orion/replication-groups/{group_id}/drain"),
                    None,
                    json!({}),
                    None,
                )
                .await?,
        ),
        GroupCommand::Delete { group_id } => print_json(
            client
                .delete(
                    &format!("/_orion/replication-groups/{group_id}"),
                    None,
                    None,
                )
                .await?,
        ),
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_group_member(value: &str) -> anyhow::Result<Value> {
    let parts = value.split(':').collect::<Vec<_>>();
    ensure!(
        (1..=3).contains(&parts.len()) && !parts[0].is_empty(),
        "group member must be node_id[:role[:priority]], got {value:?}"
    );
    let node_id = parts[0]
        .parse::<u64>()
        .with_context(|| format!("parsing member node id from {value:?}"))?;
    let role = parts
        .get(1)
        .copied()
        .filter(|role| !role.is_empty())
        .unwrap_or("voter");
    let mut member = json!({
        "node_id": node_id,
        "role": role,
    });
    if let Some(priority) = parts.get(2).filter(|priority| !priority.is_empty()) {
        member["priority"] = json!(
            priority
                .parse::<u64>()
                .with_context(|| format!("parsing member priority from {value:?}"))?
        );
    }
    Ok(member)
}

async fn explain_database_placement(client: &DbClient, name: &str) -> anyhow::Result<Value> {
    let placement = client
        .get(&format!("/_orion/databases/{name}/placement"), None)
        .await?;
    let standbys = client
        .get(
            &format!("/_orion/databases/{name}/placement/standbys"),
            None,
        )
        .await?;
    let operations = client
        .get(
            &format!("/_orion/databases/{name}/placement/operations"),
            None,
        )
        .await?;
    let metrics = client.get("/_orion/metrics/placement", None).await?;

    let standby_rows = standbys
        .get("standbys")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let operation_rows = operations
        .get("operations")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let promotable_standbys = standby_rows
        .iter()
        .filter(|standby| standby.get("promotable").and_then(Value::as_bool) == Some(true))
        .count();
    let errored_standbys = standby_rows
        .iter()
        .filter(|standby| standby.get("error").is_some_and(|error| !error.is_null()))
        .count();
    let running_operations = operation_rows
        .iter()
        .filter(|operation| operation.get("status").and_then(Value::as_str) == Some("running"))
        .count();
    let current_group = placement
        .pointer("/database/replication_group_id")
        .or_else(|| placement.pointer("/group/group_id"))
        .cloned()
        .unwrap_or(Value::Null);

    let mut warnings = Vec::new();
    if promotable_standbys == 0 {
        warnings.push(json!("no promotable standby is currently recorded"));
    }
    if errored_standbys > 0 {
        warnings.push(json!(format!(
            "{errored_standbys} standby record(s) have errors"
        )));
    }
    if running_operations > 0 {
        warnings.push(json!(format!(
            "{running_operations} placement operation(s) are running"
        )));
    }

    Ok(json!({
        "database": name,
        "current_group": current_group,
        "ready_for_dead_source_failover": promotable_standbys > 0,
        "promotable_standbys": promotable_standbys,
        "standbys_total": standby_rows.len(),
        "standbys_with_errors": errored_standbys,
        "running_operations": running_operations,
        "warnings": warnings,
        "placement": placement,
        "standbys": standbys,
        "operations": operations,
        "placement_metrics": metrics,
    }))
}

fn print_json(value: Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

const DB_USAGE: &str = r#"usage:
  orion db [--orion-url <url>] <command>

environment:
  ORION_URL         Base Orion URL, defaults to http://127.0.0.1:8091
  ORION_AUTH_TOKEN  Bearer token for operator endpoints
  LIBSQL_AUTH_TOKEN   Fallback bearer token

commands:
  list [--include-deleted]
  create <name> [--idempotency-key <key>]
  get <name>
  drop <name> [--idempotency-key <key>]
  placement <name>
  explain <name>
  plan <name> --target-group <group>
  move <name> --target-group <group> [--drain-timeout-ms <ms>]
  standbys <name>
  refresh-standby <name> --target-group <group>
  promote <name> --target-group <group> [--max-staleness-ms <ms>] [--force]
  operations <name>
  cancel <name> <operation-id> [--reason <reason>]
  repair <name> <operation-id> [--phase <phase>] [--reason <reason>]
  groups list
  groups runtime
  groups get <group-id>
  groups create <group-id> [--mode <mode>] [--member node[:role[:priority]]]...
  groups add-member <group-id> <node-id> [--role <role>] [--priority <n>]
  groups remove-member <group-id> <node-id> [--role <role>]
  groups drain <group-id>
  groups delete <group-id>
  reconcile
  standby-reconcile
  metrics

examples:
  orion db create appdb
  orion db list
  orion db groups create rg_global --member 1:voter:0 --member 2:voter:1 --member 3:voter:2
  orion db groups runtime
  orion db plan appdb --target-group rg_global
  orion db move appdb --target-group rg_dedicated --drain-timeout-ms 30000
  ORION_URL=http://127.0.0.1:8081 orion db placement appdb"#;

const GROUP_USAGE: &str = r#"usage:
  orion db groups list
  orion db groups runtime
  orion db groups get <group-id>
  orion db groups create <group-id> [--mode <mode>] [--member node[:role[:priority]]]...
  orion db groups add-member <group-id> <node-id> [--role <role>] [--priority <n>]
  orion db groups remove-member <group-id> <node-id> [--role <role>]
  orion db groups drain <group-id>
  orion db groups delete <group-id>

member roles:
  voter
  learner
  read_replica

aliases:
  groups, replication-groups, rg"#;

const GROUP_CREATE_USAGE: &str = r#"usage:
  orion db groups create <group-id>
    [--mode <mode>]
    [--no-automatic-failover]
    [--promote-after-ms <ms>]
    [--standby-targets <group-a,group-b>]
    [--member node[:role[:priority]]]...

example:
  orion db groups create rg_global \
    --mode manual \
    --member 1:voter:0 \
    --member 2:voter:1 \
    --member 3:read_replica:2"#;

const GROUP_ADD_MEMBER_USAGE: &str = r#"usage:
  orion db groups add-member <group-id> <node-id> [--role <role>] [--priority <n>]"#;

const GROUP_REMOVE_MEMBER_USAGE: &str = r#"usage:
  orion db groups remove-member <group-id> <node-id> [--role <role>]"#;
