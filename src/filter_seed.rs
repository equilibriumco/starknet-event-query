use eyre::anyhow;
use starknet_rust::core::types::Felt;

use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub struct FilterSeed {
    pub from_block: u64,
    pub to_block: u64,
    pub with_name: Option<String>,
}

impl FilterSeed {
    pub fn load(fixture: &Path) -> eyre::Result<Self> {
        let os_stem = fixture
            .file_stem()
            .ok_or_else(|| anyhow!("invalid fixture path: {:?}", fixture))?;
        let stem = os_stem
            .to_str()
            .ok_or_else(|| anyhow!("invalid fixture name: {:?}", fixture))?;
        Self::from_stem(stem)
    }

    #[allow(clippy::type_complexity)]
    pub fn get_filter_addresses_and_keys(
        &self,
        fixture: &Path,
    ) -> eyre::Result<(Vec<Felt>, Option<Vec<Vec<Felt>>>)> {
        let (raw_addresses, raw_keys) = if let Some(basename) = self.format_filter_basename() {
            let fixture_dir = fixture
                .parent()
                .ok_or_else(|| anyhow!("fixture without path: {:?}", fixture))?;
            let filter_path = fixture_dir.join(basename);
            let contents = fs::read_to_string(filter_path)?;
            let filter_map: HashMap<String, serde_json::Value> = serde_json::from_str(&contents)?;
            let raw_addresses = if let Some(raw_address) = filter_map.get("address") {
                match raw_address {
                    serde_json::Value::String(addr) => vec![addr.clone()],
                    serde_json::Value::Array(addrs) => addrs
                        .iter()
                        .map(|a| {
                            if let serde_json::Value::String(s) = a {
                                Ok(s.clone())
                            } else {
                                Err(anyhow!("unexpected address item type"))
                            }
                        })
                        .collect::<Result<Vec<String>, _>>()?,
                    _ => {
                        return Err(anyhow!("unexpected address type"));
                    }
                }
            } else {
                vec![]
            };
            let raw_keys = if let Some(serde_json::Value::Array(keys)) = filter_map.get("keys") {
                Some(keys.clone())
            } else {
                None
            };
            (raw_addresses, raw_keys)
        } else {
            (vec![], None)
        };

        let addresses = raw_addresses
            .into_iter()
            .map(|s| Felt::from_hex(&s))
            .collect::<Result<Vec<Felt>, _>>()?;

        let keys = match raw_keys {
            Some(outer) => {
                let mut key_filter = Vec::new();
                for inner in outer.into_iter() {
                    if let serde_json::Value::Array(arr) = inner {
                        let mut alt = Vec::new();
                        for v in arr {
                            if let serde_json::Value::String(k) = v {
                                alt.push(Felt::from_hex(&k)?);
                            } else {
                                return Err(anyhow!("unexpected key type"));
                            }
                        }

                        key_filter.push(alt);
                    }
                }

                Some(key_filter)
            }
            None => None,
        };

        Ok((addresses, keys))
    }

    fn from_stem(stem: &str) -> eyre::Result<Self> {
        let ret = match stem.find('+') {
            Some(pos) => {
                let from_block = str::parse::<u64>(&stem[..pos])
                    .map_err(|_| anyhow!("from block not a number"))?;
                let (block_count, with_name) = Self::parse_tail(&stem[pos + 1..])?;
                let to_block = from_block
                    .checked_add(block_count)
                    .ok_or_else(|| anyhow!("adding block count overflows"))?;
                Self {
                    from_block,
                    to_block,
                    with_name,
                }
            }
            None => {
                let (from_block, with_name) = Self::parse_tail(stem)?;
                Self {
                    from_block,
                    to_block: from_block,
                    with_name,
                }
            }
        };
        Ok(ret)
    }

    fn format_filter_basename(&self) -> Option<String> {
        if let Some(with_name) = &self.with_name {
            let head = if self.from_block == self.to_block {
                self.from_block.to_string()
            } else {
                format!("{}+{}", self.from_block, self.to_block - self.from_block)
            };

            Some(format!("{}f{}.json", head, with_name))
        } else {
            None
        }
    }

    fn parse_tail(tail: &str) -> eyre::Result<(u64, Option<String>)> {
        let pair = match tail.find('w') {
            Some(pos) => {
                let n = str::parse::<u64>(&tail[..pos])
                    .map_err(|_| anyhow!("stem tail doesn't start with a number"))?;
                let s = tail[pos + 1..].to_string();
                (n, Some(s))
            }
            None => {
                let n = str::parse::<u64>(tail).map_err(|_| anyhow!("stem tail not a number"))?;
                (n, None)
            }
        };
        Ok(pair)
    }
}
