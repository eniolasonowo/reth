//! `poolState` RPC namespace.
//!
//! Exposes the latest per-block pool tick/rates/lending snapshot gathered by
//! the pool-state ExEx (see [`crate`]): the ExEx watches swap/liquidity and
//! lending events, batches the affected pools into `eth_call`s at each chain
//! tip and publishes the decoded response on a [`watch`] channel; this
//! module serves it as `poolState_latest` and `poolState_subscribe`, plus
//! the registry mutators `poolState_addRatesPools`,
//! `poolState_addV3RatesPools` and `poolState_addLendingMarkets`.

use crate::{
    IV4Rates, LendingMarketRegistry, PoolStateSnapshot, RatesPoolRegistry, V3RatesPoolRegistry,
};
use alloy_primitives::{Address, B256};
use jsonrpsee::{core::RpcResult, proc_macros::rpc, PendingSubscriptionSink, SubscriptionMessage};
use tokio::sync::watch;

/// `poolState` namespace: latest snapshot, WS subscription and registry
/// mutators.
#[rpc(server, namespace = "poolState")]
pub trait PoolStateApi {
    /// Returns the latest pool-state snapshot, if any block has been
    /// processed yet.
    #[method(name = "latest")]
    fn latest(&self) -> RpcResult<Option<PoolStateSnapshot>>;

    /// Replaces the tracked rates pool set with the given V4 pool keys;
    /// only these pools will have `getMultiRates` data in subsequent
    /// snapshots. Returns the computed pool ids (keccak256 of the encoded
    /// key) in the same order. An empty list disables rates tracking.
    #[method(name = "addRatesPools")]
    fn add_rates_pools(&self, keys: Vec<IV4Rates::PoolKey>) -> RpcResult<Vec<B256>>;

    /// Replaces the tracked V3 rates pool address set with the given
    /// addresses; only these pools will have `getMultiRates` data in
    /// subsequent snapshots. An empty list disables V3 rates tracking.
    /// Returns the new full list.
    #[method(name = "addV3RatesPools")]
    fn add_v3_rates_pools(&self, ids: Vec<Address>) -> RpcResult<Vec<Address>>;

    /// Replaces the lending market id set passed as `allMarket` to
    /// `blockUpdate`. An empty list clears it. Returns the new full list.
    #[method(name = "addLendingMarkets")]
    fn add_lending_markets(&self, ids: Vec<B256>) -> RpcResult<Vec<B256>>;

    /// Subscribes to pool-state snapshots; one update per block that
    /// produced at least one matching event.
    #[subscription(name = "subscribe", unsubscribe = "unsubscribe", item = PoolStateSnapshot)]
    fn subscribe(&self);
}

pub struct PoolStateRpc {
    /// Latest published snapshot.
    rx: watch::Receiver<PoolStateSnapshot>,
    /// Tracked V4 rates pool keys.
    rates_pools: RatesPoolRegistry,
    /// Tracked V3 rates pool addresses.
    v3_rates_pools: V3RatesPoolRegistry,
    /// Lending market ids passed as `allMarket` to `blockUpdate`.
    lending_markets: LendingMarketRegistry,
}

impl PoolStateApiServer for PoolStateRpc {
    fn latest(&self) -> RpcResult<Option<PoolStateSnapshot>> {
        if self.rx.borrow().block_number == 0 {
            return Ok(None);
        }
        Ok(Some(self.rx.borrow().clone()))
    }

    fn add_rates_pools(&self, keys: Vec<IV4Rates::PoolKey>) -> RpcResult<Vec<B256>> {
        Ok(self.rates_pools.replace_keys(keys))
    }

    fn add_v3_rates_pools(&self, ids: Vec<Address>) -> RpcResult<Vec<Address>> {
        self.v3_rates_pools.replace_ids(ids);
        Ok(self.v3_rates_pools.ids())
    }

    fn add_lending_markets(&self, ids: Vec<B256>) -> RpcResult<Vec<B256>> {
        self.lending_markets.replace_ids(ids);
        Ok(self.lending_markets.ids())
    }

    fn subscribe(&self, pending: PendingSubscriptionSink) {
        let mut rx = self.rx.clone();
        tokio::spawn(async move {
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(e) => {
                    tracing::debug!(target: "rpc::pool_state", error = %e, "subscription closed before accept");
                    return;
                }
            };
            // Skip the initial value: subscribers only get new snapshots.
            let _ = rx.changed().await;
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let snapshot = rx.borrow_and_update().clone();
                let raw = serde_json::value::to_raw_value(&snapshot).expect("snapshot serializes");
                let msg = SubscriptionMessage::from(raw);
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Builds the `poolState` RPC module backed by the given watch receiver,
/// rates pool registries and lending market registry.
pub fn build_pool_state_rpc_module(
    rx: watch::Receiver<PoolStateSnapshot>,
    rates_pools: RatesPoolRegistry,
    v3_rates_pools: V3RatesPoolRegistry,
    lending_markets: LendingMarketRegistry,
) -> jsonrpsee::RpcModule<PoolStateRpc> {
    PoolStateRpc { rx, rates_pools, v3_rates_pools, lending_markets }.into_rpc()
}
