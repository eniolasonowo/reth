//! An Ethereum node with the pool-state ExEx installed: per-block pool
//! tick/rates/lending snapshots served over the `poolState` RPC namespace.
use alloy_primitives::{Address, B256};
use clap::{Args, Parser};
use reth_ethereum::{
    cli::Cli,
    node::{builder::NodeHandleFor, EthereumNode},
};
use reth_pool_state_exex::{
    pool_state_exex, rpc::build_pool_state_rpc_module, LendingMarketRegistry, PoolStateConfig,
    PoolStateWatch, RatesPoolRegistry, V3RatesPoolRegistry,
};

/// Extra CLI arguments for the pool-state ExEx.
#[derive(Debug, Args)]
pub struct PoolStateArgs {
    /// Enable the pool-state ExEx: per-block tick-range refresh via
    /// getMultiTicksRange, served through the poolState RPC namespace.
    #[arg(long = "exex.pool-state", default_value_t = false)]
    exex_pool_state: bool,

    /// V3-family contract exposing getMultiTicksRange (address-keyed pools).
    #[arg(
        long = "exex.pool-state.v3-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_v3_contract: Option<Address>,

    /// V4-family contract exposing getMultiTicksRange (bytes32-keyed pools).
    #[arg(
        long = "exex.pool-state.v4-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_v4_contract: Option<Address>,

    /// V4-family contract exposing getMultiRates (PoolKey-keyed pools).
    #[arg(
        long = "exex.pool-state.v4-rates-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_v4_rates_contract: Option<Address>,

    /// V3-family contract exposing getMultiRates (address-keyed pools).
    #[arg(
        long = "exex.pool-state.v3-rates-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_v3_rates_contract: Option<Address>,

    /// Liquity contract exposing getRates (protocol-keyed rows).
    #[arg(
        long = "exex.pool-state.liquity-rates-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_liquity_rates_contract: Option<Address>,

    /// Lending contract exposing blockUpdate (bytes32-keyed markets).
    #[arg(
        long = "exex.pool-state.lending-contract",
        value_name = "ADDRESS",
        requires = "exex_pool_state"
    )]
    exex_pool_state_lending_contract: Option<Address>,

    /// Topic0 filter for the pool-state ExEx; defaults to the standard
    /// V3/PancakeV3 Swap, Mint, Burn and V4 Swap, ModifyLiquidity topics.
    #[arg(
        long = "exex.pool-state.topics",
        value_name = "TOPICS",
        value_delimiter = ',',
        requires = "exex_pool_state"
    )]
    exex_pool_state_topics: Option<Vec<B256>>,
}

fn main() -> eyre::Result<()> {
    Cli::<reth_ethereum::cli::chainspec::EthereumChainSpecParser, PoolStateArgs>::parse().run(
        async move |builder, ext| {
            let pool_state_cfg = if ext.exex_pool_state {
                let contract_v3 = ext.exex_pool_state_v3_contract;
                let contract_v4 = ext.exex_pool_state_v4_contract;
                if contract_v3.is_none() && contract_v4.is_none() {
                    eyre::bail!(
                        "--exex.pool-state requires at least one of --exex.pool-state.v3-contract \
                     or --exex.pool-state.v4-contract"
                    );
                }
                PoolStateConfig::new(
                    contract_v3,
                    contract_v4,
                    ext.exex_pool_state_v4_rates_contract,
                    ext.exex_pool_state_v3_rates_contract,
                    ext.exex_pool_state_liquity_rates_contract,
                    ext.exex_pool_state_lending_contract,
                    ext.exex_pool_state_topics.clone(),
                )
            } else {
                eyre::bail!(
                    "this binary only exists to run the pool-state ExEx; pass --exex.pool-state"
                )
            };

            let (watch_tx, watch_rx) = tokio::sync::watch::channel(Default::default());
            let rates_pools = RatesPoolRegistry::new();
            let v3_rates_pools = V3RatesPoolRegistry::new();
            let lending_markets = LendingMarketRegistry::new();

            let exex_rates_pools = rates_pools.clone();
            let exex_v3_rates_pools = v3_rates_pools.clone();
            let exex_lending_markets = lending_markets.clone();

            let handle: NodeHandleFor<EthereumNode> = builder
                .node(EthereumNode::default())
                .install_exex("pool-state", move |ctx| {
                    let watch: PoolStateWatch = watch_tx.clone();
                    async move {
                        Ok(pool_state_exex(
                            ctx,
                            pool_state_cfg,
                            watch,
                            exex_rates_pools,
                            exex_v3_rates_pools,
                            exex_lending_markets,
                        ))
                    }
                })
                .extend_rpc_modules(move |ctx| {
                    ctx.modules.merge_configured(build_pool_state_rpc_module(
                        watch_rx,
                        rates_pools,
                        v3_rates_pools,
                        lending_markets,
                    ))?;
                    Ok(())
                })
                .launch()
                .await?;

            handle.wait_for_node_exit().await
        },
    )
}
