use serde::{Deserialize, Serialize};
use solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
  AccountsDbPlugin, AccountsDbPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
  ReplicaTransactionInfoVersions, Result, SlotStatus,
};
use std::sync::Arc;
use lapin::{
  options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Connection, Channel,
  ConnectionProperties,
};

#[derive(Debug, Default)]
pub struct AccountsDbPluginRabbitMq {
  channel: Option<Arc<Channel>>,

}

impl AccountsDbPlugin for AccountsDbPluginRabbitMq {
  fn name(&self) -> &'static str {
    "AccountsDbPluginRabbitMq"
  }

  fn on_load(&mut self, _config_file: &str) -> Result<()> {
    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    
    let channel = async_global_executor::block_on(async {
      let conn = Connection::connect(
          &addr,
          ConnectionProperties::default().with_default_executor(8),
      )
      .await.unwrap();

      let channel_a = conn.create_channel().await.unwrap();

      channel_a.
        queue_declare(
          "accounts",
          QueueDeclareOptions::default(),
          FieldTable::default(),
        ).await.unwrap();

      channel_a
    });

    self.channel = Some(Arc::new(channel));

    Ok(())
  }

  fn update_account(&mut self, account: ReplicaAccountInfoVersions, slot: u64, is_startup: bool) -> Result<()> {
    match account {
      ReplicaAccountInfoVersions::V0_0_1(account) => {
        println!(
          "Updating account {:?} with owner {:?}",
        account.pubkey, account.owner);
        let channel = self.channel.as_ref().cloned();
        let pubkey = account.pubkey.to_owned();
        async_global_executor::spawn(async move {
          channel.unwrap()
                  .basic_publish(
                      "",
                      "accounts",
                      BasicPublishOptions::default(),
                      pubkey.into(),
                      BasicProperties::default(),
                  )
                  .await.unwrap()
                  .await.unwrap();
          }).detach();
      }
    }
  Ok(())
  }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
pub unsafe extern "C" fn _create_plugin() -> *mut dyn AccountsDbPlugin {
  let plugin = AccountsDbPluginRabbitMq::default();
  let plugin: Box<dyn AccountsDbPlugin> = Box::new(plugin);
  Box::into_raw(plugin)
}
