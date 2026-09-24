use std::time::Duration;

use crate::application::{
    adapter_runtime::{AdapterLauncher, AdapterRetirement, AdapterSession, LaunchAdapter},
    providers::{ProviderLaunchResolver, ProviderLaunchTarget},
};

mod codec;
mod error;
mod process;
mod session;

pub use error::AdapterProcessError;
use process::{ProviderProcess, validate_process_instance_id};
pub use session::{AdapterCatalogReadStream, AdapterProcess, AdapterStateReadStream};

#[derive(Clone)]
pub struct AdapterProcessLauncher<R> {
    resolver: R,
    host_version: String,
    ready_timeout: Duration,
    rpc_timeout: Duration,
    shutdown_timeout: Duration,
}

impl<R> AdapterProcessLauncher<R>
where
    R: ProviderLaunchResolver,
{
    pub fn new(resolver: R, host_version: impl Into<String>) -> Self {
        Self {
            resolver,
            host_version: host_version.into(),
            ready_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_timeouts(
        mut self,
        ready_timeout: Duration,
        rpc_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Self {
        self.ready_timeout = ready_timeout;
        self.rpc_timeout = rpc_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    async fn launch_process(
        &self,
        command: LaunchAdapter,
    ) -> Result<AdapterProcess, AdapterProcessError> {
        validate_process_instance_id(&command.process_instance_id)?;
        let target = self.resolver.resolve_provider(&command.provider_id).await?;
        self.launch_target(target, command.process_instance_id)
            .await
    }

    async fn launch_target(
        &self,
        target: ProviderLaunchTarget,
        process_instance_id: String,
    ) -> Result<AdapterProcess, AdapterProcessError> {
        let process = ProviderProcess::launch(
            target,
            process_instance_id,
            &self.host_version,
            self.ready_timeout,
            self.rpc_timeout,
            self.shutdown_timeout,
        )
        .await?;
        Ok(AdapterProcess::new(process))
    }

    pub(crate) async fn probe_target(
        &self,
        target: ProviderLaunchTarget,
        process_instance_id: String,
    ) -> Result<(), AdapterProcessError> {
        let process = self.launch_target(target, process_instance_id).await?;
        if process.retire().await? != AdapterRetirement::Graceful {
            return Err(AdapterProcessError::ProbeShutdown);
        }
        Ok(())
    }
}

impl<R> AdapterLauncher for AdapterProcessLauncher<R>
where
    R: ProviderLaunchResolver,
{
    type Session = AdapterProcess;
    type Error = AdapterProcessError;

    async fn launch(&self, command: LaunchAdapter) -> Result<Self::Session, Self::Error> {
        self.launch_process(command).await
    }
}
