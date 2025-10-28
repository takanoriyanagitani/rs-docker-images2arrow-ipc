use std::io::stdout;
use std::process::ExitCode;

use clap::Parser;

use rs_docker_images2arrow_ipc::DOCKER_CLIENT_VERSION_DEFAULT;
use rs_docker_images2arrow_ipc::DOCKER_CON_TIMEOUT_SECONDS_DEFAULT;
use rs_docker_images2arrow_ipc::DOCKER_UNIX_PATH_DEFAULT;
use rs_docker_images2arrow_ipc::images2writer;
use rs_docker_images2arrow_ipc::unix2docker;

/// A simple CLI to get Docker images and output them as an Arrow IPC stream.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the Docker Unix socket.
    #[arg(long, default_value = DOCKER_UNIX_PATH_DEFAULT)]
    docker_sock_path: String,

    /// Timeout for Docker connection in seconds.
    #[arg(long, default_value_t = DOCKER_CON_TIMEOUT_SECONDS_DEFAULT)]
    docker_conn_timeout: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let docker = match unix2docker(
        &cli.docker_sock_path,
        cli.docker_conn_timeout,
        DOCKER_CLIENT_VERSION_DEFAULT,
    ) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to connect to Docker: {}", e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = images2writer(&docker, stdout(), None).await {
        eprintln!("Failed to write images: {}", e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
