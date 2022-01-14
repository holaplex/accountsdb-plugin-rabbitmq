use {log::*, std::collections::HashSet};

#[derive(Debug)]
pub(crate) struct AccountsSelector {
    pub owners: HashSet<Vec<u8>>,
}

impl AccountsSelector {
    pub fn default() -> Self {
        AccountsSelector {
            owners: HashSet::default(),
        }
    }

    pub fn new(owners: &[String]) -> Self {
        info!("Creating AccountsSelector from owners: {:?}", owners);

        let owners = owners
            .iter()
            .map(|key| bs58::decode(key).into_vec().unwrap())
            .collect();
        AccountsSelector { owners }
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.owners.contains(owner)
    }

    /// Check if any account is of interested at all
    pub fn is_enabled(&self) -> bool {
        !self.owners.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_create_accounts_selector() {
        AccountsSelector::new(&["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()]);

        AccountsSelector::new(&["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()]);
    }
}
