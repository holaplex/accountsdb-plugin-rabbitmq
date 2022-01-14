use lapin::{
    options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Channel,
    Connection, ConnectionProperties, ExchangeKind,
};
use serde::{Deserialize, Serialize};
use solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
    AccountsDbPlugin, AccountsDbPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result, SlotStatus,
};
use std::sync::Arc;

use {
    crate::{account_selector::AccountsSelector, transaction_selector::TransactionsSelector},
    log::*,
    serde_json,
    std::{fs::File, io::Read},
};

#[derive(Debug, Default)]
pub struct AccountsDbPluginRabbitMq {
    channel: Option<Arc<Channel>>,
    accounts_selector: Option<AccountsSelector>,
    transactions_selector: Option<TransactionsSelector>,
}

impl AccountsDbPlugin for AccountsDbPluginRabbitMq {
    fn name(&self) -> &'static str {
        "AccountsDbPluginRabbitMq"
    }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        println!("{}", config_file);
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let result: serde_json::Value = serde_json::from_str(&contents).unwrap();

        self.accounts_selector = Some(Self::create_accounts_selector_from_config(&result));
        self.transactions_selector = Some(Self::create_transactions_selector_from_config(&result));

        let addr = std::env::var("AMQP_ADDR").unwrap();

        let channel = async_global_executor::block_on(async {
            let conn = Connection::connect(
                &addr,
                ConnectionProperties::default().with_default_executor(8),
            )
            .await
            .unwrap();

            let channel = conn.create_channel().await.unwrap();

            channel
                .exchange_declare(
                    "accounts",
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            channel
        });

        self.channel = Some(Arc::new(channel));

        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        _slot: u64,
    ) -> Result<()> {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_transaction) => {
                if let Some(transactions_selector) = &self.transactions_selector {
                    if !transactions_selector.is_transaction_selected(&[]) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                if let Some(accounts_selector) = &self.accounts_selector {
                    if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }

                println!(
                    "Updating account {:?} with owner {:?}",
                    bs58::encode(account.pubkey).into_string(),
                    bs58::encode(account.owner).into_string(),
                );

                let channel = self.channel.as_ref().cloned();
                let pubkey = account.pubkey.to_owned();
                let mut payload = vec![];
                payload.extend(pubkey);
                payload.extend(account.owner);
                payload.extend(slot.to_le_bytes());
                payload.extend(account.data);
                async_global_executor::spawn(async move {
                    channel
                        .unwrap()
                        .basic_publish(
                            "accounts",
                            "",
                            BasicPublishOptions::default(),
                            payload,
                            BasicProperties::default(),
                        )
                        .await
                        .unwrap()
                        .await
                        .unwrap();
                })
                .detach();
            }
        }
        Ok(())
    }
}

impl AccountsDbPluginRabbitMq {
    fn create_accounts_selector_from_config(config: &serde_json::Value) -> AccountsSelector {
        let accounts_selector = &config["accounts_selector"];

        if accounts_selector.is_null() {
            AccountsSelector::default()
        } else {
            let owners = &accounts_selector["owners"];
            let owners: Vec<String> = if owners.is_array() {
                owners
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            AccountsSelector::new(&owners)
        }
    }

    fn create_transactions_selector_from_config(
        config: &serde_json::Value,
    ) -> TransactionsSelector {
        let transaction_selector = &config["transaction_selector"];

        if transaction_selector.is_null() {
            TransactionsSelector::default()
        } else {
            let owners = &transaction_selector["owners"];
            let owners: Vec<String> = if owners.is_array() {
                owners
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            TransactionsSelector::new(&owners)
        }
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
