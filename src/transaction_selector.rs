use {log::*, std::collections::HashSet};

#[derive(Debug)]
pub(crate) struct TransactionsSelector {
    pub owners: HashSet<Vec<u8>>,
}

impl TransactionsSelector {
    pub fn default() -> Self {
        TransactionsSelector {
            owners: HashSet::default(),
        }
    }

    pub fn new(owners: &[String]) -> Self {
        info!("Creating TransactionsSelector from owners: {:?}", owners);

        let owners = owners
            .iter()
            .map(|key| bs58::decode(key).into_vec().unwrap())
            .collect();
        TransactionsSelector { owners }
    }

    pub fn is_transaction_selected(&self, owner: &[u8]) -> bool {
        self.owners.contains(owner)
    }

    /// Check if any account is of interested at all
    pub fn is_enabled(&self) -> bool {
        !self.owners.is_empty()
    }
}
