use anyhow::Context;
use std::path::Path;

mod cli;
mod db_cli;
mod libsql_http;
mod node;
mod storage_node;

use node::{DefaultConfig, NodeConfig, run_node};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match command_from_args()? {
        Command::Server { config_path } => {
            let used_default_config = config_path.is_none();
            let (config, source) = match config_path {
                Some(config_path) => {
                    let config = NodeConfig::from_yaml_file(&config_path).with_context(|| {
                        format!("loading node config {}", config_path.display())
                    })?;
                    (config, format!("config file {}", config_path.display()))
                }
                None => {
                    let config = DefaultConfig::one_node();
                    (
                        config,
                        "built-in DefaultConfig::one_node() (no config path provided)".to_string(),
                    )
                }
            };
            println!("{}", config.human_summary(&source));
            if used_default_config {
                println!("Tip: run `orion init-config` to write this as an editable YAML file.");
                println!();
            }
            if used_default_config {
                let raft_log_root = config.raft_log_root();
                let object_store = config.storage.object_store.label();
                run_node(config).await.with_context(|| {
                    format!(
                        "starting default one-node cluster. This uses persisted dev state at storage.local.raft_log_root={raft_log_root} and storage.object_store={object_store}; if the on-disk format changed during development, stop orion and remove ./data or choose fresh paths in a YAML config"
                    )
                })
            } else {
                run_node(config).await
            }
        }
        Command::InitConfig { output_path, force } => write_initial_config(&output_path, force),
        Command::Cli { url } => cli::run_cli(url).await,
        Command::Db { args } => db_cli::run_db_cli(args).await,
        Command::Help { usage } => {
            println!("{usage}");
            Ok(())
        }
    }
}

enum Command {
    Server {
        config_path: Option<std::path::PathBuf>,
    },
    InitConfig {
        output_path: std::path::PathBuf,
        force: bool,
    },
    Cli {
        url: Option<String>,
    },
    Db {
        args: Vec<String>,
    },
    Help {
        usage: &'static str,
    },
}

fn command_from_args() -> anyhow::Result<Command> {
    let mut args = std::env::args().skip(1);
    let Some(first) = args.next() else {
        return Ok(Command::Help { usage: USAGE });
    };

    match first.as_str() {
        "server" => server_command_from_args(args),
        "db" => Ok(Command::Db {
            args: args.collect(),
        }),
        "cli" | "shell" => {
            let url = args.next();
            ensure_no_extra_args(args)?;
            Ok(Command::Cli { url })
        }
        "init-config" => {
            let mut output_path = std::path::PathBuf::from("orion.yaml");
            let mut force = false;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--force" | "-f" => force = true,
                    "--output" | "-o" => {
                        let path = args.next().ok_or_else(|| {
                            anyhow::anyhow!("init-config requires a path after {arg}")
                        })?;
                        output_path = path.into();
                    }
                    "--help" | "-h" => {
                        return Err(anyhow::anyhow!(INIT_CONFIG_USAGE));
                    }
                    path if !path.starts_with('-') => output_path = path.into(),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "unknown init-config argument {arg:?}\n\n{INIT_CONFIG_USAGE}"
                        ));
                    }
                }
            }
            Ok(Command::InitConfig { output_path, force })
        }
        "--config" | "-c" => Err(anyhow::anyhow!(
            "server startup now uses `orion server --config <node.yaml>`\n\n{USAGE}"
        )),
        "--help" | "-h" => Ok(Command::Help { usage: USAGE }),
        path if !path.starts_with('-') => Err(anyhow::anyhow!(
            "unknown command {path:?}; to start a server with a config file, use `orion server {path}`\n\n{USAGE}"
        )),
        _ => Err(anyhow::anyhow!("unknown argument {first:?}\n\n{USAGE}")),
    }
}

fn server_command_from_args(mut args: impl Iterator<Item = String>) -> anyhow::Result<Command> {
    let Some(first) = args.next() else {
        return Ok(Command::Server { config_path: None });
    };

    match first.as_str() {
        "--config" | "-c" => {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("server --config requires a path"))?;
            ensure_no_extra_args(args)?;
            Ok(Command::Server {
                config_path: Some(path.into()),
            })
        }
        "--help" | "-h" => Ok(Command::Help {
            usage: SERVER_USAGE,
        }),
        path if !path.starts_with('-') => {
            ensure_no_extra_args(args)?;
            Ok(Command::Server {
                config_path: Some(path.into()),
            })
        }
        _ => Err(anyhow::anyhow!(
            "unknown server argument {first:?}\n\n{SERVER_USAGE}"
        )),
    }
}

fn ensure_no_extra_args(mut args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    if let Some(arg) = args.next() {
        anyhow::bail!("unexpected argument {arg:?}\n\n{USAGE}");
    }
    Ok(())
}

fn write_initial_config(output_path: &Path, force: bool) -> anyhow::Result<()> {
    if output_path.exists() && !force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite it",
            output_path.display()
        );
    }

    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config directory {}", parent.display()))?;
    }
    std::fs::write(output_path, NodeConfig::commented_example_yaml())
        .with_context(|| format!("writing config {}", output_path.display()))?;
    println!("wrote {}", output_path.display());
    Ok(())
}

const USAGE: &str = r#"usage:
  orion server [--config <node.yaml>]
  orion db <command>            Manage databases and placement
  orion cli [url]               Open the libSQL shell
  orion init-config [path]      Write a commented starter config

examples:
  orion server
  orion server --config ./orion.yaml
  orion db create appdb
  orion db list
  orion cli
  orion init-config
  orion init-config ./orion.yaml"#;

const SERVER_USAGE: &str = r#"usage:
  orion server                         Start one local single-node server with defaults
  orion server --config <node.yaml>    Start with a YAML config
  orion server <node.yaml>             Start with a YAML config"#;

const INIT_CONFIG_USAGE: &str = r#"usage:
  orion init-config [path]
  orion init-config --output <path>

options:
  -o, --output <path>  Config path to write; defaults to orion.yaml
  -f, --force          Overwrite an existing file"#;
