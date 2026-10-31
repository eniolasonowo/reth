//! Pool-state ExEx.
//!
//! On every committed chain, this execution extension:
//!
//! 1. Filters block logs by the configured topic0s (V3/PancakeV3 `Swap`, `Mint`, `Burn`, V4 `Swap`,
//!    `ModifyLiquidity`, and Morpho-family lending events).
//! 2. Selects one tick per pool: the last `Swap` tick outranks liquidity events; otherwise the last
//!    `Mint`/`Burn`/`ModifyLiquidity` `tickLower`.
//! 3. Batches all selected pools into a single `getMultiTicksRange` `eth_call` pinned to the new
//!    chain tip, plus `getMultiRates` calls (V4 PoolKey- and V3 address-keyed), a Liquity
//!    `getRates` call and a lending `blockUpdate` call when configured.
//! 4. Publishes the decoded responses as a [`PoolStateSnapshot`] on a [`watch`] channel served by
//!    the `poolState` RPC namespace (see [`rpc`]).
//!
//! The ExEx is generic over any [`FullNodeComponents`] whose primitives are
//! [`EthPrimitives`], so it runs on stock Ethereum nodes as well as custom
//! chains.

use alloy_consensus::{BlockHeader as _, TxEip1559, TxReceipt as _};
use alloy_primitives::{Address, Bytes, Log, Signed, TxKind, B256, I256, U160, U256};
use alloy_sol_types::{SolCall, SolEvent};
use futures::TryStreamExt;
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::{ConfigureEvm, Evm as _, EvmFactory, EvmFor};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::{FullNodeComponents, NodeTypes};
use reth_primitives_traits::{HeaderTy, Recovered};
use reth_provider::{Chain, StateProviderBox, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, db::State};
use revm::context_interface::{result::ResultAndState, Block as _};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::watch;

/// Tracing target used by this crate.
const TARGET: &str = "exex::pool_state";

/// UniswapV3 `Swap(address,address,int256,int256,uint160,uint128,int24)`.
pub const TOPIC_UNISWAP_V3_SWAP: B256 =
    alloy_primitives::b256!("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");

/// PancakeV3 `Swap(address indexed sender,address indexed recipient,int256 amount0,int256
/// amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick,uint128 protocolFeesToken0,uint128
/// protocolFeesToken1)`.
pub const TOPIC_PANCAKE_V3_SWAP: B256 =
    alloy_primitives::b256!("0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83");

/// V3 `Mint(address,address indexed owner,int24 indexed tickLower,int24 indexed
/// tickUpper,uint128,uint256,uint256)`.
pub const TOPIC_V3_MINT: B256 =
    alloy_primitives::b256!("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde");

/// V3 `Burn(address indexed owner,int24 indexed tickLower,int24 indexed
/// tickUpper,uint128,uint256,uint256)`.
pub const TOPIC_V3_BURN: B256 =
    alloy_primitives::b256!("0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c");

/// UniswapV4 `ModifyLiquidity(bytes32 indexed id,address indexed sender,int24 tickLower,int24
/// tickUpper,int256 liquidityDelta)`.
pub const TOPIC_V4_MODIFY_LIQUIDITY: B256 =
    alloy_primitives::b256!("0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec");

/// UniswapV4 `Swap(bytes32 indexed id,address indexed sender,int128 amount0,int128 amount1,uint160
/// sqrtPriceX96,uint128 liquidity,int24 tick,uint24 fee)`.
pub const TOPIC_V4_SWAP: B256 =
    alloy_primitives::b256!("0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f");

/// Morpho `Supply(bytes32 indexed id, address indexed caller, address indexed onBehalf, uint256
/// assets, uint256 shares)`.
pub const TOPIC_LENDING_SUPPLY: B256 =
    alloy_primitives::b256!("0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0");

/// Morpho `Withdraw(bytes32 indexed id, address caller, address indexed onBehalf, address indexed
/// receiver, uint256 assets, uint256 shares)`.
pub const TOPIC_LENDING_WITHDRAW: B256 =
    alloy_primitives::b256!("0xa56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf");

/// Morpho `Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed
/// receiver, uint256 assets, uint256 shares)`.
pub const TOPIC_LENDING_BORROW: B256 =
    alloy_primitives::b256!("0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43");

/// Morpho `Repay(bytes32 indexed id, address indexed caller, address indexed onBehalf, uint256
/// assets, uint256 shares)`.
pub const TOPIC_LENDING_REPAY: B256 =
    alloy_primitives::b256!("0x52acb05cebbd3cd39715469f22afbf5a17496295ef3bc9bb5944056c63ccaa09");

/// Morpho `SupplyCollateral(bytes32 indexed id, address indexed caller, address indexed onBehalf,
/// uint256 assets)`.
pub const TOPIC_LENDING_SUPPLY_COLLATERAL: B256 =
    alloy_primitives::b256!("0xa3b9472a1399e17e123f3c2e6586c23e504184d504de59cdaa2b375e880c6184");

/// Morpho `WithdrawCollateral(bytes32 indexed id, address caller, address indexed onBehalf, address
/// indexed receiver, uint256 assets)`.
pub const TOPIC_LENDING_WITHDRAW_COLLATERAL: B256 =
    alloy_primitives::b256!("0xe80ebd7cc9223d7382aab2e0d1d6155c65651f83d53c8b9b06901d167e321142");

/// Default topic0 filter used when no explicit topics are configured.
pub fn default_topics() -> Vec<B256> {
    vec![
        TOPIC_UNISWAP_V3_SWAP,
        TOPIC_PANCAKE_V3_SWAP,
        TOPIC_V3_MINT,
        TOPIC_V3_BURN,
        TOPIC_V4_MODIFY_LIQUIDITY,
        TOPIC_V4_SWAP,
    ]
}

/// V3-family event bindings (UniswapV3 / PancakeV3 share signatures).
mod v3_events {
    alloy_sol_types::sol! {
        #[derive(Debug)]
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick
        );

        #[derive(Debug)]
        event Mint(
            address sender,
            address indexed owner,
            int24 indexed tickLower,
            int24 indexed tickUpper,
            uint128 amount,
            uint256 amount0,
            uint256 amount1
        );

        #[derive(Debug)]
        event Burn(
            address indexed owner,
            int24 indexed tickLower,
            int24 indexed tickUpper,
            uint128 amount,
            uint256 amount0,
            uint256 amount1
        );
    }
}

/// PancakeV3 event bindings (signature extends UniswapV3's Swap with two
/// trailing protocol-fee fields, hence a different topic0).
mod pancake_events {
    alloy_sol_types::sol! {
        #[derive(Debug)]
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint128 protocolFeesToken0,
            uint128 protocolFeesToken1
        );
    }
}

/// V4 event bindings.
mod v4_events {
    alloy_sol_types::sol! {
        #[derive(Debug)]
        event Swap(
            bytes32 indexed id,
            address indexed sender,
            int128 amount0,
            int128 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint24 fee
        );

        #[derive(Debug)]
        event ModifyLiquidity(
            bytes32 indexed id,
            address indexed sender,
            int24 tickLower,
            int24 tickUpper,
            int256 liquidityDelta,
            bytes32 extra
        );
    }
}

/// Lending (Morpho-family) event bindings.
mod lending_events {
    alloy_sol_types::sol! {
        #[derive(Debug)]
        event Supply(
            bytes32 indexed id,
            address indexed caller,
            address indexed onBehalf,
            uint256 assets,
            uint256 shares
        );

        #[derive(Debug)]
        event Withdraw(
            bytes32 indexed id,
            address caller,
            address indexed onBehalf,
            address indexed receiver,
            uint256 assets,
            uint256 shares
        );

        #[derive(Debug)]
        event Borrow(
            bytes32 indexed id,
            address caller,
            address indexed onBehalf,
            address indexed receiver,
            uint256 assets,
            uint256 shares
        );

        #[derive(Debug)]
        event Repay(
            bytes32 indexed id,
            address indexed caller,
            address indexed onBehalf,
            uint256 assets,
            uint256 shares
        );

        #[derive(Debug)]
        event SupplyCollateral(
            bytes32 indexed id,
            address indexed caller,
            address indexed onBehalf,
            uint256 assets
        );

        #[derive(Debug)]
        event WithdrawCollateral(
            bytes32 indexed id,
            address caller,
            address indexed onBehalf,
            address indexed receiver,
            uint256 assets
        );
    }
}

// V3-family tick-range contract: pools are identified by address.
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface IV3PoolState {
        struct ITicksRangeArgs {
            address pool;
            int24 tick;
        }
        struct IRangeTickInfo {
            int24 tickIndex;
            int128 liquidityNet;
        }
        struct IRangeTickInfoLpFee {
            uint24 lpFee;
            IRangeTickInfo[] range;
        }
        function getMultiTicksRange(ITicksRangeArgs[] memory args)
            external
            view
            returns (IRangeTickInfoLpFee[] memory multiPoolInfo);
    }
}

// V4-family tick-range contract: pools are identified by bytes32 id.
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface IV4PoolState {
        struct ITicksRangeArgs {
            bytes32 poolId;
            int24 tick;
        }
        struct IRangeTickInfo {
            int24 tickIndex;
            int128 liquidityNet;
        }
        struct IRangeTickInfoLpFee {
            uint24 lpFee;
            IRangeTickInfo[] range;
        }
        function getMultiTicksRange(ITicksRangeArgs[] memory args)
            external
            view
            returns (IRangeTickInfoLpFee[] memory multiPoolInfo);
    }
}

// V4-family rates contract: pools are identified by PoolKey (its
// keccak256-abi-encoded hash is the pool id).
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface IV4Rates {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct PoolKey {
            address currency0;
            address currency1;
            uint24 fee;
            int24 tickSpacing;
            address hooks;
        }
        struct Rates {
            bytes32 poolId;
            uint256 rate0In;
            uint256 delta0;
            uint256 rate1In;
            uint256 delta1;
            uint256[] rates0Out;
            uint256[] rates1Out;
        }
        function getMultiRates(PoolKey[] memory keys)
            public
            returns (Rates[] memory rates);
    }
}

// V3-family rates contract: pools are identified by address.
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface IV3Rates {
        struct Rates {
            address poolAddress;
            uint256 rate0In;
            uint256 delta0;
            uint256 rate1In;
            uint256 delta1;
            uint256[] rates0Out;
            uint256[] rates1Out;
        }
        function getMultiRates(address[] memory poolAddresses)
            public
            returns (Rates[] memory rates);
    }
}

// Liquity rates contract: no arguments; the contract returns its own
// configured rows, one per protocol with six data columns.
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface ILiquityRates {
        struct Out {
            uint256 protocol;
            uint256[] col0;
            uint256[] col1;
            uint256[] col2;
            uint256[] col3;
            uint256[] col4;
            uint256[] col5;
        }
        function getRates() public returns (Out[] memory outs);
    }
}

// Lending block-update contract: markets are identified by bytes32 id.
alloy_sol_types::sol! {
    #[derive(Debug)]
    interface ILendingBlockUpdate {
        struct UserPosition {
            bytes32 id;
            address user;
            uint256 supplyShares;
            uint128 borrowShares;
            uint128 collateral;
        }

        struct MarketDetail {
            bytes32 id;
            // market
            uint128 totalSupplyAssets;
            uint128 totalSupplyShares;
            uint128 totalBorrowAssets;
            uint128 totalBorrowShares;
            uint128 lastUpdate;
            uint128 fee;
            // market params
            address loanToken;
            address collateralToken;
            address oracle;
            address irm;
            uint256 lltv;
            // irm borrow rate
            uint256 borrowRate;
            // oracle price
            uint256 price;
        }

        struct BlockUpdate {
            MarketDetail[] marketDetails;
            UserPosition[] positions;
        }

        function blockUpdate(
            bytes32[] memory allMarket,
            bytes32[] memory updatedMarket,
            address[] memory users
        ) external returns (BlockUpdate memory update);
    }
}

/// One tick entry of a pool's range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickRangeEntry {
    /// Tick index.
    pub tick_index: i32,
    /// Net liquidity at the tick, decimal string (int128).
    pub liquidity_net: String,
}

/// Per-pool tick-range state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolStateEntry {
    /// Pool id: V4 raw `bytes32` id, or a left-padded V2/V3 pool address.
    pub pool_id: String,
    /// Tick that triggered the refresh.
    pub tick: i32,
    /// Pool fee in hundredths of a bip.
    pub lp_fee: u32,

    /// SqrtPriceX96
    pub sqrtprice_x96: U160,
    /// liquidity
    pub liquidity: u128,
    pub amount0: I256,
    pub amount1: I256,
    /// Tick range around the trigger tick.
    pub range: Vec<TickRangeEntry>,
}

/// Per-pool rates from `getMultiRates`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolRatesEntry {
    /// V4 raw `bytes32` pool id or V3 pool address.
    pub pool_id: String,
    /// Input rate for token0, decimal string (uint256).
    pub rate0_in: String,
    /// Delta for token0, decimal string (uint256).
    pub delta0: String,
    /// Input rate for token1, decimal string (uint256).
    pub rate1_in: String,
    /// Delta for token1, decimal string (uint256).
    pub delta1: String,
    /// Output rates for token0, decimal strings (uint256).
    pub rates0_out: Vec<String>,
    /// Output rates for token1, decimal strings (uint256).
    pub rates1_out: Vec<String>,
}

/// One row of the Liquity `getRates` table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiquityRatesEntry {
    /// Protocol identifier, decimal string (uint256).
    pub protocol: String,
    /// Column 0 values, decimal strings (uint256).
    pub col0: Vec<String>,
    /// Column 1 values, decimal strings (uint256).
    pub col1: Vec<String>,
    /// Column 2 values, decimal strings (uint256).
    pub col2: Vec<String>,
    /// Column 3 values, decimal strings (uint256).
    pub col3: Vec<String>,
    /// Column 4 values, decimal strings (uint256).
    pub col4: Vec<String>,
    /// Column 5 values, decimal strings (uint256).
    pub col5: Vec<String>,
}

/// One user position of a lending market.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPositionEntry {
    /// Morpho market id (raw `bytes32`).
    pub id: String,
    /// Owner of the position.
    pub user: String,
    /// Supply shares, decimal string (uint256).
    pub supply_shares: String,
    /// Borrow shares, decimal string (uint128).
    pub borrow_shares: String,
    /// Collateral, decimal string (uint128).
    pub collateral: String,
}

/// Per-market detail returned by `blockUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDetailEntry {
    /// Morpho market id (raw `bytes32`).
    pub id: String,
    /// Total supply assets, decimal string (uint128).
    pub total_supply_assets: String,
    /// Total supply shares, decimal string (uint128).
    pub total_supply_shares: String,
    /// Total borrow assets, decimal string (uint128).
    pub total_borrow_assets: String,
    /// Total borrow shares, decimal string (uint128).
    pub total_borrow_shares: String,
    /// Last accrual timestamp, decimal string (uint128).
    pub last_update: String,
    /// Market fee, decimal string (uint128).
    pub fee: String,
    /// Loan token.
    pub loan_token: String,
    /// Collateral token.
    pub collateral_token: String,
    /// Oracle.
    pub oracle: String,
    /// Interest rate model.
    pub irm: String,
    /// Liquidation LTV, decimal string (uint256).
    pub lltv: String,
    /// IRM borrow rate, decimal string (uint256).
    pub borrow_rate: String,
    /// Oracle price, decimal string (uint256).
    pub price: String,
}

/// Lending data from `blockUpdate`, nested in the pool-state snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LendingBlockUpdateData {
    /// Market details for the ids seen in the block's lending events.
    pub market_details: Vec<MarketDetailEntry>,
    /// Positions for the `onBehalf` users seen in the block's lending events.
    pub positions: Vec<UserPositionEntry>,
}

impl LendingBlockUpdateData {
    /// True when neither market details nor positions were produced.
    pub fn is_empty(&self) -> bool {
        self.market_details.is_empty() && self.positions.is_empty()
    }
}

/// Latest pool-state snapshot published by the ExEx for one block.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PoolStateSnapshot {
    /// Block the snapshot was computed at (0 until the first block).
    pub block_number: u64,
    /// Hash of the block the snapshot was computed at.
    pub block_hash: String,

    /// Wall-clock nanoseconds since the Unix epoch, captured when the ExEx
    /// began processing the notification that produced this snapshot.
    pub timestamp: u128,
    /// Base fee of the block the snapshot was computed at (0 if pre-London).
    pub base_fee: u64,

    /// One entry per pool that had a matching event in the block.
    pub entries: Vec<PoolStateEntry>,
    /// One entry per pool with a configured rates contract; empty if no
    /// rates contract is configured or the call failed.
    pub rates: Vec<PoolRatesEntry>,
    /// Liquity rates from `getRates`; empty if the Liquity contract is
    /// not configured or the call failed.
    pub liquity_rates: Vec<LiquityRatesEntry>,
    /// Lending data from `blockUpdate`; empty if the lending contract is
    /// not configured or the call failed.
    pub lending: LendingBlockUpdateData,
}

/// Watch channel used to publish snapshots from the ExEx to the RPC layer.
pub type PoolStateWatch = watch::Sender<PoolStateSnapshot>;

/// Shared registry of V4 pools eligible for `getMultiRates` data, mapping
/// pool id (keccak256-abi-encoded PoolKey) to its key. Cheap to clone;
/// clones share the same underlying map.
#[derive(Debug, Default)]
pub struct RatesPoolRegistry {
    ids: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<B256, IV4Rates::PoolKey>>>,
}

impl Clone for RatesPoolRegistry {
    fn clone(&self) -> Self {
        Self { ids: std::sync::Arc::clone(&self.ids) }
    }
}

impl RatesPoolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the tracked pool set with the given keys (deduped by the
    /// computed pool id). An empty slice disables rates tracking. Returns
    /// the computed pool ids in the same order.
    pub fn replace_keys(&self, keys: Vec<IV4Rates::PoolKey>) -> Vec<B256> {
        let mut map = std::collections::HashMap::with_capacity(keys.len());
        let ids = keys
            .into_iter()
            .map(|key| {
                let id = pool_key_id(&key);
                map.insert(id, key);
                id
            })
            .collect();
        *self.ids.write().expect("rates pool registry poisoned") = map;
        ids
    }

    /// Returns the currently tracked pool ids (sorted for a stable order).
    pub fn ids(&self) -> Vec<B256> {
        let map = self.ids.read().expect("rates pool registry poisoned");
        map.keys().copied().collect()
    }

    /// True when the pool id is tracked.
    pub fn contains_id(&self, id: &B256) -> bool {
        self.ids.read().expect("rates pool registry poisoned").contains_key(id)
    }

    /// Returns the keys for the given pool ids, in the given order; ids
    /// missing from the registry are skipped.
    pub fn keys_for(&self, pool_ids: &[B256]) -> Vec<IV4Rates::PoolKey> {
        let map = self.ids.read().expect("rates pool registry poisoned");
        pool_ids.iter().filter_map(|id| map.get(id).cloned()).collect()
    }
}

/// Computes the V4 pool id of a PoolKey: keccak256(abi.encode(key)), the
/// same as v4-core's `PoolIdLibrary.toId`.
pub fn pool_key_id(key: &IV4Rates::PoolKey) -> B256 {
    use alloy_sol_types::SolValue;
    alloy_primitives::keccak256(key.abi_encode())
}

/// Shared registry of V3 pool addresses eligible for `getMultiRates` data.
/// Cheap to clone; clones share the same underlying set.
#[derive(Debug, Default)]
pub struct V3RatesPoolRegistry {
    ids: std::sync::Arc<std::sync::RwLock<std::collections::HashSet<Address>>>,
}

impl Clone for V3RatesPoolRegistry {
    fn clone(&self) -> Self {
        Self { ids: std::sync::Arc::clone(&self.ids) }
    }
}

impl V3RatesPoolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the tracked pool address set with the given ids (deduped).
    /// An empty slice disables V3 rates tracking.
    pub fn replace_ids(&self, ids: Vec<Address>) {
        let mut set = self.ids.write().expect("v3 rates pool registry poisoned");
        set.clear();
        set.extend(ids);
    }

    /// Returns the currently tracked pool addresses (sorted for a stable order).
    pub fn ids(&self) -> Vec<Address> {
        let set = self.ids.read().expect("v3 rates pool registry poisoned");
        set.iter().copied().collect()
    }

    /// True when the pool address is tracked.
    pub fn contains_id(&self, id: &Address) -> bool {
        self.ids.read().expect("v3 rates pool registry poisoned").contains(id)
    }
}

/// Shared registry of lending market ids supplied as `allMarket` to
/// `blockUpdate`. Cheap to clone; clones share the same underlying set.
#[derive(Debug, Default)]
pub struct LendingMarketRegistry {
    ids: std::sync::Arc<std::sync::RwLock<std::collections::HashSet<B256>>>,
}

impl Clone for LendingMarketRegistry {
    fn clone(&self) -> Self {
        Self { ids: std::sync::Arc::clone(&self.ids) }
    }
}

impl LendingMarketRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the tracked market id set with the given ids (deduped).
    /// An empty slice disables lending market tracking.
    pub fn replace_ids(&self, ids: Vec<B256>) {
        let mut set = self.ids.write().expect("lending market registry poisoned");
        set.clear();
        set.extend(ids);
    }

    /// Returns the currently tracked market ids (sorted for a stable order).
    pub fn ids(&self) -> Vec<B256> {
        let set = self.ids.read().expect("lending market registry poisoned");
        set.iter().copied().collect()
    }
}

/// Pool-state ExEx configuration.
#[derive(Debug, Clone)]
pub struct PoolStateConfig {
    /// V3-family contract exposing `getMultiTicksRange` (address-keyed pools).
    pub contract_v3: Option<Address>,
    /// V4-family contract exposing `getMultiTicksRange` (bytes32-keyed pools).
    pub contract_v4: Option<Address>,
    /// V4-family contract exposing `getMultiRates` (PoolKey-keyed pools).
    pub contract_v4_rates: Option<Address>,
    /// V3-family contract exposing `getMultiRates` (address-keyed pools).
    pub contract_v3_rates: Option<Address>,
    /// Liquity contract exposing `getRates` (protocol-keyed rows).
    pub contract_liquity_rates: Option<Address>,
    /// Lending contract exposing `blockUpdate` (bytes32-keyed markets).
    pub contract_lending: Option<Address>,
    /// Topic0 filter; defaults to [`default_topics`].
    pub topics: Vec<B256>,
}

impl PoolStateConfig {
    /// Builds a config, falling back to the default topic set. Families
    /// without a configured contract are not tracked.
    pub fn new(
        contract_v3: Option<Address>,
        contract_v4: Option<Address>,
        contract_v4_rates: Option<Address>,
        contract_v3_rates: Option<Address>,
        contract_liquity_rates: Option<Address>,
        contract_lending: Option<Address>,
        topics: Option<Vec<B256>>,
    ) -> Self {
        Self {
            contract_v3,
            contract_v4,
            contract_v4_rates,
            contract_v3_rates,
            contract_liquity_rates,
            contract_lending,
            topics: topics.unwrap_or_else(default_topics),
        }
    }
}

/// Selected tick for a pool during a block scan.
#[derive(Debug, Clone, Copy)]
struct Selection {
    tick: i32,
    is_swap: bool,
    /// Swap market data (zero for liquidity-only pools).
    sqrt_price_x96: U160,
    liquidity: u128,
    amount0: I256,
    amount1: I256,
}

impl Selection {
    fn swap(
        tick: i32,
        sqrt_price_x96: U160,
        liquidity: u128,
        amount0: I256,
        amount1: I256,
    ) -> Self {
        Self { tick, is_swap: true, sqrt_price_x96, liquidity, amount0, amount1 }
    }

    fn liquidity_event(tick: i32) -> Self {
        Self {
            tick,
            is_swap: false,
            sqrt_price_x96: U160::ZERO,
            liquidity: 0,
            amount0: I256::ZERO,
            amount1: I256::ZERO,
        }
    }
}

/// Pool-state ExEx entry point.
pub async fn pool_state_exex<N>(
    mut ctx: ExExContext<N>,
    cfg: PoolStateConfig,
    watch: PoolStateWatch,
    rates_pools: RatesPoolRegistry,
    v3_rates_pools: V3RatesPoolRegistry,
    lending_markets: LendingMarketRegistry,
) -> eyre::Result<()>
where
    N: FullNodeComponents<
        Provider: StateProviderFactory + Clone + Unpin + 'static,
        Types: NodeTypes<Primitives = EthPrimitives>,
    >,
    <N::Evm as ConfigureEvm>::BlockExecutorFactory: alloy_evm::block::BlockExecutorFactory<
        EvmFactory: EvmFactory<Tx: reth_evm::FromRecoveredTx<TxEip1559>>,
    >,
{
    let exex_id = "pool-state";

    tracing::info!(
        target: TARGET,
        contract_v3 = ?cfg.contract_v3,
        contract_v4 = ?cfg.contract_v4,
        contract_liquity_rates = ?cfg.contract_liquity_rates,
        contract_lending = ?cfg.contract_lending,
        topics = cfg.topics.len(),
        "pool-state ExEx started"
    );

    while let Some(notification) = ctx.notifications.try_next().await? {
        match &notification {
            ExExNotification::ChainCommitted { new } => {
                process_chain::<N>(
                    new,
                    &cfg,
                    ctx.components.evm_config(),
                    ctx.components.provider(),
                    &watch,
                    &rates_pools,
                    &v3_rates_pools,
                    &lending_markets,
                );
            }
            ExExNotification::ChainReorged { .. } => {
                // Snapshot is recomputed from the next committed chain.
            }
            ExExNotification::ChainReverted { .. } => {
                // State will be refreshed by the next committed chain.
            }
        }

        if let Some(committed_chain) = notification.committed_chain() {
            ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }
    }

    tracing::info!(target: TARGET, id = exex_id, "notification stream closed");
    Ok(())
}

/// Scans a committed chain for matching events, batches one
/// `getMultiTicksRange` call at the chain tip and publishes the snapshot.
fn process_chain<N>(
    chain: &Chain<EthPrimitives>,
    cfg: &PoolStateConfig,
    evm_config: &N::Evm,
    provider: &N::Provider,
    watch: &PoolStateWatch,
    rates_pools: &RatesPoolRegistry,
    v3_rates_pools: &V3RatesPoolRegistry,
    lending_markets: &LendingMarketRegistry,
) where
    N: FullNodeComponents<
        Provider: StateProviderFactory + Clone + Unpin + 'static,
        Types: NodeTypes<Primitives = EthPrimitives>,
    >,
    <N::Evm as ConfigureEvm>::BlockExecutorFactory: alloy_evm::block::BlockExecutorFactory<
        EvmFactory: EvmFactory<Tx: reth_evm::FromRecoveredTx<TxEip1559>>,
    >,
{
    // Per-family pool selections; a family without a configured contract is
    // not tracked at all.
    let mut selections_v3: BTreeMap<Address, Selection> = BTreeMap::new();
    let mut selections_v4: BTreeMap<B256, Selection> = BTreeMap::new();
    // Lending-family selections: unique market ids and unique `onBehalf`
    // users; not tracked without a configured lending contract.
    let mut lending_ids: BTreeSet<B256> = BTreeSet::new();
    let mut lending_users: BTreeSet<Address> = BTreeSet::new();
    for block in chain.blocks_iter() {
        let Some(receipts) = chain.receipts_by_block_hash(block.hash()) else {
            continue;
        };
        for receipt in receipts {
            for log in receipt.logs() {
                process_log(log, cfg, &mut selections_v3, &mut selections_v4);
                process_lending_log(log, cfg, &mut lending_ids, &mut lending_users);
            }
        }
    }

    // Lending block-update call: independent of the pool ticks/rates calls.
    // Runs on every committed block — the contract returns market details
    // even when no lending events matched (empty updatedMarket/users); the
    // ids/users gathered from the block's events enrich the call with the
    // positions actually touched by that block, while allMarket comes from
    // the RPC-managed lending market registry.
    let lending_data = if cfg.contract_lending.is_some() {
        process_lending_block_update(
            chain,
            cfg,
            evm_config,
            provider,
            lending_markets,
            &lending_ids,
            &lending_users,
        )
    } else {
        LendingBlockUpdateData::default()
    };

    // Liquity rates call: independent of the pool ticks/rates calls. Runs
    // on every committed block — the contract returns its full configured
    // table regardless of the block's events.
    let liquity_rates = if cfg.contract_liquity_rates.is_some() {
        process_liquity_rates(chain, cfg, evm_config, provider)
    } else {
        Vec::new()
    };

    if selections_v3.is_empty() && selections_v4.is_empty() {
        if lending_data.is_empty() && liquity_rates.is_empty() {
            return;
        }
        // Lending/liquity-only block: publish the snapshot with empty pool
        // entries.
        let tip = chain.tip();
        let snapshot = PoolStateSnapshot {
            block_number: tip.number(),
            block_hash: format!("{:#x}", tip.hash()),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default(),
            base_fee: tip.header().base_fee_per_gas().unwrap_or_default(),
            entries: Vec::new(),
            rates: Vec::new(),
            liquity_rates,
            lending: lending_data,
        };
        tracing::debug!(
            target: TARGET,
            block_number = snapshot.block_number,
            markets = snapshot.lending.market_details.len(),
            liquity_rows = snapshot.liquity_rates.len(),
            "published lending/liquity-only pool-state snapshot"
        );
        watch.send_replace(snapshot);
        return;
    }

    let tip = chain.tip();
    let mut entries: Vec<PoolStateEntry> = Vec::new();

    let (mut call_evm, chain_id, base_fee) =
        match build_call_evm(evm_config, provider, tip.header(), tip.hash()) {
            Ok((evm, chain_id, base_fee)) => (evm, chain_id, base_fee),
            Err(err) => {
                tracing::warn!(
                    target: TARGET,
                    block_number = tip.number(),
                    error = %err,
                    "failed to build call evm; keeping previous snapshot"
                );
                return;
            }
        };
    let started_at_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();

    if let (Some(contract), false) = (cfg.contract_v3, selections_v3.is_empty()) {
        let args: Vec<IV3PoolState::ITicksRangeArgs> = selections_v3
            .iter()
            .map(|(pool, sel)| IV3PoolState::ITicksRangeArgs {
                pool: *pool,
                tick: Signed::<24, 1>::try_from(sel.tick).unwrap_or_default(),
            })
            .collect();

        let call_started = std::time::Instant::now();
        match call_contract(
            &mut call_evm,
            evm_config,
            contract,
            IV3PoolState::getMultiTicksRangeCall { args }.abi_encode().into(),
            chain_id,
        ) {
            Ok(output) => {
                tracing::info!(
                    target: TARGET,
                    family = "v3",
                    block_number = tip.number(),
                    pools = selections_v3.len(),
                    elapsed_ms = call_started.elapsed().as_millis() as u64,
                    "getMultiTicksRange call completed"
                );
                match IV3PoolState::getMultiTicksRangeCall::abi_decode_returns(&output) {
                    Ok(returns) => {
                        entries.extend(selections_v3.iter().zip(returns).map(
                            |((pool, sel), info)| {
                                PoolStateEntry {
                                    pool_id: format!("{pool:#x}"),
                                    tick: sel.tick,
                                    lp_fee: u32::try_from(info.lpFee).unwrap_or_default(),
                                    sqrtprice_x96: sel.sqrt_price_x96,
                                    liquidity: sel.liquidity,
                                    amount0: sel.amount0,
                                    amount1: sel.amount1,
                                    range: info
                                        .range
                                        .iter()
                                        .map(|tick| TickRangeEntry {
                                            tick_index: i32::try_from(tick.tickIndex)
                                                .unwrap_or_default(),
                                            liquidity_net: tick.liquidityNet.to_string(),
                                        })
                                        .collect(),
                                }
                            },
                        ));
                    }
                    Err(err) => tracing::warn!(
                        target: TARGET,
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode v3 getMultiTicksRange output"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                target: TARGET,
                family = "v3",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "v3 getMultiTicksRange call failed; keeping previous snapshot"
            ),
        }
    }

    if let (Some(contract), false) = (cfg.contract_v4, selections_v4.is_empty()) {
        let args: Vec<IV4PoolState::ITicksRangeArgs> = selections_v4
            .iter()
            .map(|(pool_id, sel)| IV4PoolState::ITicksRangeArgs {
                poolId: *pool_id,
                tick: Signed::<24, 1>::try_from(sel.tick).unwrap_or_default(),
            })
            .collect();

        let call_started = std::time::Instant::now();
        match call_contract(
            &mut call_evm,
            evm_config,
            contract,
            IV4PoolState::getMultiTicksRangeCall { args }.abi_encode().into(),
            chain_id,
        ) {
            Ok(output) => {
                tracing::info!(
                    target: TARGET,
                    family = "v4",
                    block_number = tip.number(),
                    pools = selections_v4.len(),
                    elapsed_ms = call_started.elapsed().as_millis() as u64,
                    "getMultiTicksRange call completed"
                );
                match IV4PoolState::getMultiTicksRangeCall::abi_decode_returns(&output) {
                    Ok(returns) => {
                        entries.extend(selections_v4.iter().zip(returns).map(
                            |((pool_id, sel), info)| {
                                PoolStateEntry {
                                    pool_id: format!("{pool_id:#x}"),
                                    tick: sel.tick,
                                    lp_fee: u32::try_from(info.lpFee).unwrap_or_default(),
                                    sqrtprice_x96: sel.sqrt_price_x96,
                                    liquidity: sel.liquidity,
                                    amount0: sel.amount0,
                                    amount1: sel.amount1,
                                    range: info
                                        .range
                                        .iter()
                                        .map(|tick| TickRangeEntry {
                                            tick_index: i32::try_from(tick.tickIndex)
                                                .unwrap_or_default(),
                                            liquidity_net: tick.liquidityNet.to_string(),
                                        })
                                        .collect(),
                                }
                            },
                        ));
                    }
                    Err(err) => tracing::warn!(
                        target: TARGET,
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode v4 getMultiTicksRange output"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                target: TARGET,
                family = "v4",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "v4 getMultiTicksRange call failed; keeping previous snapshot"
            ),
        }
    }

    // Rates call for the V4 pools; independent of the ticks call outcome.
    let mut rates: Vec<PoolRatesEntry> = Vec::new();
    // Only pools both touched by this block's events and tracked via
    // `poolState_addRatesPools` get rates data; an empty registry disables
    // the rates call entirely.
    let rates_pool_ids: Vec<B256> =
        selections_v4.keys().filter(|id| rates_pools.contains_id(id)).copied().collect();
    if let (Some(rates_contract), false) = (cfg.contract_v4_rates, rates_pool_ids.is_empty()) {
        let call_keys = rates_pools.keys_for(&rates_pool_ids);

        let call_started = std::time::Instant::now();
        match call_contract(
            &mut call_evm,
            evm_config,
            rates_contract,
            IV4Rates::getMultiRatesCall { keys: call_keys }.abi_encode().into(),
            chain_id,
        ) {
            Ok(output) => {
                tracing::info!(
                    target: TARGET,
                    family = "v4",
                    call = "getMultiRates",
                    block_number = tip.number(),
                    pools = rates_pool_ids.len(),
                    elapsed_ms = call_started.elapsed().as_millis() as u64,
                    "getMultiRates call completed"
                );
                match IV4Rates::getMultiRatesCall::abi_decode_returns(&output) {
                    Ok(returns) => {
                        rates.extend(returns.iter().map(|r| PoolRatesEntry {
                            pool_id: format!("{:#x}", r.poolId),
                            rate0_in: r.rate0In.to_string(),
                            delta0: r.delta0.to_string(),
                            rate1_in: r.rate1In.to_string(),
                            delta1: r.delta1.to_string(),
                            rates0_out: r.rates0Out.iter().map(|v| v.to_string()).collect(),
                            rates1_out: r.rates1Out.iter().map(|v| v.to_string()).collect(),
                        }));
                    }
                    Err(err) => tracing::warn!(
                        target: TARGET,
                        family = "v4",
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode v4 getMultiRates output; publishing ticks without rates"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                target: TARGET,
                family = "v4",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "v4 getMultiRates call failed; publishing ticks without rates"
            ),
        }
    }

    // V3 rates: only pools both touched by this block's events and tracked
    // via `poolState_addV3RatesPools` get rates data; an empty registry
    // disables the call entirely.
    let v3_rates_pool_ids: Vec<Address> =
        selections_v3.keys().filter(|pool| v3_rates_pools.contains_id(pool)).copied().collect();
    if let (Some(rates_contract), false) = (cfg.contract_v3_rates, v3_rates_pool_ids.is_empty()) {
        let call_pool_ids = v3_rates_pool_ids.clone();
        let call_started = std::time::Instant::now();
        match call_contract(
            &mut call_evm,
            evm_config,
            rates_contract,
            IV3Rates::getMultiRatesCall { poolAddresses: call_pool_ids }.abi_encode().into(),
            chain_id,
        ) {
            Ok(output) => {
                tracing::info!(
                    target: TARGET,
                    family = "v3",
                    call = "getMultiRates",
                    block_number = tip.number(),
                    pools = v3_rates_pool_ids.len(),
                    elapsed_ms = call_started.elapsed().as_millis() as u64,
                    "getMultiRates call completed"
                );
                match IV3Rates::getMultiRatesCall::abi_decode_returns(&output) {
                    Ok(returns) => {
                        rates.extend(returns.iter().map(|r| PoolRatesEntry {
                            pool_id: format!("{:#x}", r.poolAddress),
                            rate0_in: r.rate0In.to_string(),
                            delta0: r.delta0.to_string(),
                            rate1_in: r.rate1In.to_string(),
                            delta1: r.delta1.to_string(),
                            rates0_out: r.rates0Out.iter().map(|v| v.to_string()).collect(),
                            rates1_out: r.rates1Out.iter().map(|v| v.to_string()).collect(),
                        }));
                    }
                    Err(err) => tracing::warn!(
                        target: TARGET,
                        family = "v3",
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode v3 getMultiRates output; publishing ticks without rates"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                target: TARGET,
                family = "v3",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "v3 getMultiRates call failed; publishing ticks without rates"
            ),
        }
    }

    if entries.is_empty() && rates.is_empty() && liquity_rates.is_empty() && lending_data.is_empty()
    {
        return;
    }

    let snapshot = PoolStateSnapshot {
        block_number: tip.number(),
        block_hash: format!("{:#x}", tip.hash()),
        timestamp: started_at_ns,
        base_fee,
        entries,
        rates,
        liquity_rates,
        lending: lending_data,
    };

    tracing::debug!(
        target: TARGET,
        block_number = snapshot.block_number,
        pools = snapshot.entries.len(),
        "published pool-state snapshot"
    );
    watch.send_replace(snapshot);
}

/// Applies one log to the per-family selection maps. V3-family pools are
/// keyed by address, V4-family pools by bytes32 id; families without a
/// configured contract are skipped.
fn process_log(
    log: &Log,
    cfg: &PoolStateConfig,
    selections_v3: &mut BTreeMap<Address, Selection>,
    selections_v4: &mut BTreeMap<B256, Selection>,
) {
    let Some(&topic0) = log.topics().first() else {
        return;
    };
    if !cfg.topics.contains(&topic0) {
        return;
    }

    let insert_swap =
        |selections: &mut BTreeMap<B256, Selection>, pool_id: B256, sel: Selection| {
            selections.insert(pool_id, sel);
        };
    let insert_liquidity =
        |selections: &mut BTreeMap<B256, Selection>, pool_id: B256, tick: i32| {
            match selections.get(&pool_id) {
                // A swap always outranks liquidity events for the same pool.
                Some(sel) if sel.is_swap => {}
                _ => {
                    selections.insert(pool_id, Selection::liquidity_event(tick));
                }
            }
        };
    let insert_v3_swap =
        |selections: &mut BTreeMap<Address, Selection>, pool: Address, sel: Selection| {
            selections.insert(pool, sel);
        };
    let insert_v3_liquidity =
        |selections: &mut BTreeMap<Address, Selection>, pool: Address, tick: i32| {
            match selections.get(&pool) {
                // A swap always outranks liquidity events for the same pool.
                Some(sel) if sel.is_swap => {}
                _ => {
                    selections.insert(pool, Selection::liquidity_event(tick));
                }
            }
        };

    let to_i32 = |v: Signed<24, 1>| i32::try_from(v).unwrap_or_default();
    match topic0 {
        t if t == TOPIC_UNISWAP_V3_SWAP => {
            match (cfg.contract_v3, v3_events::Swap::decode_log(log)) {
                (Some(_), Ok(event)) => insert_v3_swap(
                    selections_v3,
                    log.address,
                    Selection::swap(
                        to_i32(event.tick),
                        event.sqrtPriceX96,
                        event.liquidity,
                        event.amount0,
                        event.amount1,
                    ),
                ),
                (Some(_), Err(err)) => {
                    tracing::debug!(target: TARGET, error = %err, "failed to decode v3 Swap log")
                }
                (None, _) => {}
            }
        }
        t if t == TOPIC_PANCAKE_V3_SWAP => {
            match (cfg.contract_v3, pancake_events::Swap::decode_log(log)) {
                (Some(_), Ok(event)) => insert_v3_swap(
                    selections_v3,
                    log.address,
                    Selection::swap(
                        to_i32(event.tick),
                        event.sqrtPriceX96,
                        event.liquidity,
                        event.amount0,
                        event.amount1,
                    ),
                ),
                (Some(_), Err(err)) => {
                    tracing::debug!(target: TARGET, error = %err, "failed to decode pancake v3 Swap log")
                }
                (None, _) => {}
            }
        }
        t if t == TOPIC_V3_MINT => match (cfg.contract_v3, v3_events::Mint::decode_log(log)) {
            (Some(_), Ok(event)) => {
                insert_v3_liquidity(selections_v3, log.address, to_i32(event.tickLower))
            }
            (Some(_), Err(err)) => {
                tracing::debug!(target: TARGET, error = %err, "failed to decode v3 Mint log")
            }
            (None, _) => {}
        },
        t if t == TOPIC_V3_BURN => match (cfg.contract_v3, v3_events::Burn::decode_log(log)) {
            (Some(_), Ok(event)) => {
                insert_v3_liquidity(selections_v3, log.address, to_i32(event.tickLower))
            }
            (Some(_), Err(err)) => {
                tracing::debug!(target: TARGET, error = %err, "failed to decode v3 Burn log")
            }
            (None, _) => {}
        },
        t if t == TOPIC_V4_SWAP => match (cfg.contract_v4, v4_events::Swap::decode_log(log)) {
            (Some(_), Ok(event)) => insert_swap(
                selections_v4,
                event.id,
                Selection::swap(
                    to_i32(event.tick),
                    event.sqrtPriceX96,
                    event.liquidity,
                    I256::try_from(event.amount0).unwrap_or_default(),
                    I256::try_from(event.amount1).unwrap_or_default(),
                ),
            ),
            (Some(_), Err(err)) => {
                tracing::debug!(target: TARGET, error = %err, "failed to decode v4 Swap log")
            }
            (None, _) => {}
        },
        t if t == TOPIC_V4_MODIFY_LIQUIDITY => {
            match (cfg.contract_v4, v4_events::ModifyLiquidity::decode_log(log)) {
                (Some(_), Ok(event)) => {
                    insert_liquidity(selections_v4, event.id, to_i32(event.tickLower))
                }
                (Some(_), Err(err)) => tracing::debug!(
                    target: TARGET,
                    error = %err,
                    "failed to decode v4 ModifyLiquidity log"
                ),
                (None, _) => {}
            }
        }
        _ => {}
    }
}

/// Applies one log to the lending id/user sets. Market ids are keyed by the
/// events' indexed `id`; users by the indexed `onBehalf` (independent lists
/// for the `blockUpdate` call). No-op without a configured lending contract.
fn process_lending_log(
    log: &Log,
    cfg: &PoolStateConfig,
    lending_ids: &mut BTreeSet<B256>,
    lending_users: &mut BTreeSet<Address>,
) {
    let Some(&topic0) = log.topics().first() else {
        return;
    };
    if cfg.contract_lending.is_none() {
        return;
    }

    let mut record = |id: B256, on_behalf: Address| {
        lending_ids.insert(id);
        lending_users.insert(on_behalf);
    };

    match topic0 {
        t if t == TOPIC_LENDING_SUPPLY => {
            if let Ok(event) = lending_events::Supply::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        t if t == TOPIC_LENDING_WITHDRAW => {
            if let Ok(event) = lending_events::Withdraw::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        t if t == TOPIC_LENDING_BORROW => {
            if let Ok(event) = lending_events::Borrow::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        t if t == TOPIC_LENDING_REPAY => {
            if let Ok(event) = lending_events::Repay::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        t if t == TOPIC_LENDING_SUPPLY_COLLATERAL => {
            if let Ok(event) = lending_events::SupplyCollateral::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        t if t == TOPIC_LENDING_WITHDRAW_COLLATERAL => {
            if let Ok(event) = lending_events::WithdrawCollateral::decode_log(log) {
                record(event.id, event.onBehalf);
            }
        }
        _ => {}
    }
}

/// Batches the registry-supplied allMarket ids and the block's
/// updatedMarket ids / onBehalf users into a single `blockUpdate` call
/// pinned to the new chain tip; returns the decoded data, or default on
/// failure.
///
/// Called on every committed block when the lending contract is configured:
/// the contract returns market details even when no lending events matched,
/// so the snapshot stays fresh block-to-block regardless of lending
/// activity.
#[allow(clippy::too_many_arguments)]
fn process_lending_block_update<E>(
    chain: &Chain<EthPrimitives>,
    cfg: &PoolStateConfig,
    evm_config: &E,
    provider: &(impl StateProviderFactory + Clone + Unpin + 'static),
    lending_markets: &LendingMarketRegistry,
    lending_ids: &BTreeSet<B256>,
    lending_users: &BTreeSet<Address>,
) -> LendingBlockUpdateData
where
    E: ConfigureEvm<Primitives = EthPrimitives>,
    E::BlockExecutorFactory: alloy_evm::block::BlockExecutorFactory<
        EvmFactory: EvmFactory<Tx: reth_evm::FromRecoveredTx<TxEip1559>>,
    >,
{
    let Some(contract) = cfg.contract_lending else {
        return LendingBlockUpdateData::default();
    };
    let tip = chain.tip();

    let (mut call_evm, chain_id, _base_fee) =
        match build_call_evm(evm_config, provider, tip.header(), tip.hash()) {
            Ok(built) => built,
            Err(err) => {
                tracing::warn!(
                    target: TARGET,
                    block_number = tip.number(),
                    error = %err,
                    "failed to build call evm; skipping lending block update"
                );
                return LendingBlockUpdateData::default();
            }
        };

    let all_market: Vec<B256> = lending_markets.ids();
    let all_market_len = all_market.len();
    let updated_market: Vec<B256> = lending_ids.iter().copied().collect();
    let users: Vec<Address> = lending_users.iter().copied().collect();
    let call_started = std::time::Instant::now();
    match call_contract(
        &mut call_evm,
        evm_config,
        contract,
        ILendingBlockUpdate::blockUpdateCall {
            allMarket: all_market,
            updatedMarket: updated_market,
            users,
        }
        .abi_encode()
        .into(),
        chain_id,
    ) {
        Ok(output) => {
            tracing::info!(
                target: TARGET,
                family = "lending",
                call = "blockUpdate",
                block_number = tip.number(),
                markets = all_market_len,
                updated = lending_ids.len(),
                users = lending_users.len(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                "blockUpdate call completed"
            );
            match ILendingBlockUpdate::blockUpdateCall::abi_decode_returns(&output) {
                Ok(update) => LendingBlockUpdateData {
                    market_details: update
                        .marketDetails
                        .iter()
                        .map(|m| MarketDetailEntry {
                            id: format!("{:#x}", m.id),
                            total_supply_assets: m.totalSupplyAssets.to_string(),
                            total_supply_shares: m.totalSupplyShares.to_string(),
                            total_borrow_assets: m.totalBorrowAssets.to_string(),
                            total_borrow_shares: m.totalBorrowShares.to_string(),
                            last_update: m.lastUpdate.to_string(),
                            fee: m.fee.to_string(),
                            loan_token: format!("{:#x}", m.loanToken),
                            collateral_token: format!("{:#x}", m.collateralToken),
                            oracle: format!("{:#x}", m.oracle),
                            irm: format!("{:#x}", m.irm),
                            lltv: m.lltv.to_string(),
                            borrow_rate: m.borrowRate.to_string(),
                            price: m.price.to_string(),
                        })
                        .collect(),
                    positions: update
                        .positions
                        .iter()
                        .map(|p| UserPositionEntry {
                            id: format!("{:#x}", p.id),
                            user: format!("{:#x}", p.user),
                            supply_shares: p.supplyShares.to_string(),
                            borrow_shares: p.borrowShares.to_string(),
                            collateral: p.collateral.to_string(),
                        })
                        .collect(),
                },
                Err(err) => {
                    tracing::warn!(
                        target: TARGET,
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode blockUpdate output; publishing pools without lending data"
                    );
                    LendingBlockUpdateData::default()
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                target: TARGET,
                family = "lending",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "blockUpdate call failed; publishing pools without lending data"
            );
            LendingBlockUpdateData::default()
        }
    }
}

/// Calls the Liquity `getRates` view pinned to the new chain tip and
/// returns the decoded rows, or empty on failure.
///
/// Called on every committed block when the Liquity contract is configured:
/// the contract takes no arguments and returns its full configured table
/// regardless of the block's events, so the snapshot stays fresh
/// block-to-block.
fn process_liquity_rates<E>(
    chain: &Chain<EthPrimitives>,
    cfg: &PoolStateConfig,
    evm_config: &E,
    provider: &(impl StateProviderFactory + Clone + Unpin + 'static),
) -> Vec<LiquityRatesEntry>
where
    E: ConfigureEvm<Primitives = EthPrimitives>,
    E::BlockExecutorFactory: alloy_evm::block::BlockExecutorFactory<
        EvmFactory: EvmFactory<Tx: reth_evm::FromRecoveredTx<TxEip1559>>,
    >,
{
    let Some(contract) = cfg.contract_liquity_rates else {
        return Vec::new();
    };
    let tip = chain.tip();

    let (mut call_evm, chain_id, _base_fee) =
        match build_call_evm(evm_config, provider, tip.header(), tip.hash()) {
            Ok(built) => built,
            Err(err) => {
                tracing::warn!(
                    target: TARGET,
                    block_number = tip.number(),
                    error = %err,
                    "failed to build call evm; skipping liquity rates"
                );
                return Vec::new();
            }
        };

    let call_started = std::time::Instant::now();
    match call_contract(
        &mut call_evm,
        evm_config,
        contract,
        ILiquityRates::getRatesCall {}.abi_encode().into(),
        chain_id,
    ) {
        Ok(output) => {
            tracing::info!(
                target: TARGET,
                family = "liquity",
                call = "getRates",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                "getRates call completed"
            );
            match ILiquityRates::getRatesCall::abi_decode_returns(&output) {
                Ok(outs) => outs.iter().map(liquity_entry).collect(),
                Err(err) => {
                    tracing::warn!(
                        target: TARGET,
                        block_number = tip.number(),
                        error = %err,
                        "failed to decode getRates output; publishing pools without liquity rates"
                    );
                    Vec::new()
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                target: TARGET,
                family = "liquity",
                block_number = tip.number(),
                elapsed_ms = call_started.elapsed().as_millis() as u64,
                error = %err,
                "getRates call failed; publishing pools without liquity rates"
            );
            Vec::new()
        }
    }
}

/// Maps one decoded Liquity `getRates` row to its snapshot entry; every
/// uint256 becomes a decimal string (JS-safe).
fn liquity_entry(out: &ILiquityRates::Out) -> LiquityRatesEntry {
    LiquityRatesEntry {
        protocol: out.protocol.to_string(),
        col0: liquity_col(&out.col0),
        col1: liquity_col(&out.col1),
        col2: liquity_col(&out.col2),
        col3: liquity_col(&out.col3),
        col4: liquity_col(&out.col4),
        col5: liquity_col(&out.col5),
    }
}

/// Formats one `getRates` column as decimal strings.
fn liquity_col(col: &[U256]) -> Vec<String> {
    col.iter().map(U256::to_string).collect()
}

/// The shared EVM instance used for the per-chain `eth_call`s: revm `State`
/// over the tip's historical state provider.
type CallEvm<E> = EvmFor<E, State<StateProviderDatabase<StateProviderBox>>>;

/// Builds the EVM instance shared by all of a chain's calls, pinned to the
/// state at the given block.
///
/// Mirrors reth's `eth_call` handling (see `prepare_call_env` in
/// `rpc-eth-api/src/helpers/call.rs`): same cfg relaxations.
fn build_call_evm<E>(
    evm_config: &E,
    provider: &(impl StateProviderFactory + Clone + Unpin + 'static),
    header: &HeaderTy<E::Primitives>,
    at: B256,
) -> eyre::Result<(CallEvm<E>, u64 /* chain_id */, u64 /* base_fee */)>
where
    E: ConfigureEvm<Primitives = EthPrimitives>,
{
    let state = provider.history_by_block_hash(at)?;

    let mut evm_env = evm_config.evm_env(header)?;

    let chain_id = evm_env.cfg_env.chain_id;
    // Captured before `disable_base_fee` relaxes the env below.
    let base_fee = evm_env.block_env.basefee();

    // Same relaxations `prepare_call_env` applies for `eth_call`.
    evm_env.cfg_env.disable_nonce_check = true;
    evm_env.cfg_env.disable_base_fee = true;
    evm_env.cfg_env.disable_eip3607 = true;
    evm_env.cfg_env.disable_block_gas_limit = true;
    evm_env.cfg_env.disable_fee_charge = true;
    evm_env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);

    let db = State::builder().with_database(StateProviderDatabase::new(state)).build();
    Ok((evm_config.evm_with_env(db, evm_env), chain_id, base_fee))
}

/// Executes an arbitrary call on the shared EVM: sender `0x0`, zero gas
/// price, unlimited gas. The call may leave (no-op) state changes in the
/// `State` overlay, which is fine for view contracts. Returns the raw output.
fn call_contract<E>(
    evm: &mut CallEvm<E>,
    evm_config: &E,
    contract: Address,
    calldata: Bytes,
    chain_id: u64,
) -> eyre::Result<Bytes>
where
    E: ConfigureEvm<Primitives = EthPrimitives>,
    E::BlockExecutorFactory: alloy_evm::block::BlockExecutorFactory<
        EvmFactory: EvmFactory<Tx: reth_evm::FromRecoveredTx<TxEip1559>>,
    >,
{
    let tx = TxEip1559 {
        gas_limit: u64::MAX,
        to: TxKind::Call(contract),
        input: calldata,
        // zero fees: no balance needed for the zeroed caller
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        chain_id,
        ..Default::default()
    };

    let tx_env = evm_config.tx_env(Recovered::new_unchecked(tx, Address::ZERO));
    let ResultAndState { result, .. } = evm.transact_raw(tx_env)?;
    result.into_output().ok_or_else(|| eyre::eyre!("call returned no output"))
}

pub mod rpc;

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolEvent;

    /// Each topic0 filter constant must equal the signature hash of the event
    /// binding used to decode it; otherwise logs would silently stop matching.
    #[test]
    fn topic_constants_match_event_bindings() {
        assert_eq!(
            TOPIC_UNISWAP_V3_SWAP,
            v3_events::Swap::SIGNATURE_HASH,
            "uniswap v3 swap topic drifted from its binding"
        );
        assert_ne!(
            TOPIC_PANCAKE_V3_SWAP, TOPIC_UNISWAP_V3_SWAP,
            "pancake v3 swap topic must differ from uniswap v3's"
        );
        assert_eq!(
            TOPIC_PANCAKE_V3_SWAP,
            pancake_events::Swap::SIGNATURE_HASH,
            "pancake v3 swap topic drifted from its binding"
        );
        assert_eq!(TOPIC_V3_MINT, v3_events::Mint::SIGNATURE_HASH);
        assert_eq!(TOPIC_V3_BURN, v3_events::Burn::SIGNATURE_HASH);
        assert_eq!(TOPIC_V4_SWAP, v4_events::Swap::SIGNATURE_HASH);
        assert_eq!(TOPIC_V4_MODIFY_LIQUIDITY, v4_events::ModifyLiquidity::SIGNATURE_HASH);
    }

    /// The default topic filter must contain every supported topic exactly once.
    #[test]
    fn default_topics_cover_all_supported() {
        let mut topics = default_topics();
        topics.sort();
        topics.dedup();
        assert_eq!(topics.len(), default_topics().len(), "default topic list contains duplicates");
        for expected in [
            TOPIC_UNISWAP_V3_SWAP,
            TOPIC_PANCAKE_V3_SWAP,
            TOPIC_V3_MINT,
            TOPIC_V3_BURN,
            TOPIC_V4_MODIFY_LIQUIDITY,
            TOPIC_V4_SWAP,
        ] {
            assert!(default_topics().contains(&expected), "default topic list missing {expected}");
        }
    }

    /// Swap selections carry market data; liquidity-only selections default
    /// to zeros; and a liquidity event never clobbers an earlier swap's data.
    #[test]
    fn selection_market_data_semantics() {
        let sqrt = U160::from(1_234_567_890u64);
        let amt0 = I256::try_from(-100_000_000i64).unwrap();
        let amt1 = I256::try_from(500_000_000i64).unwrap();

        let swap = Selection::swap(-887220, sqrt, 987_654u128, amt0, amt1);
        assert!(swap.is_swap);
        assert_eq!(swap.tick, -887220);
        assert_eq!(swap.sqrt_price_x96, sqrt);
        assert_eq!(swap.liquidity, 987_654);
        assert_eq!(swap.amount0, amt0);
        assert_eq!(swap.amount1, amt1);

        let liq = Selection::liquidity_event(42);
        assert!(!liq.is_swap);
        assert_eq!(liq.sqrt_price_x96, U160::ZERO);
        assert_eq!(liq.liquidity, 0);
        assert_eq!(liq.amount0, I256::ZERO);
        assert_eq!(liq.amount1, I256::ZERO);

        // Same precedence rule as `insert_v3_liquidity`/`insert_liquidity`:
        // a liquidity event must not overwrite an existing swap selection.
        let mut pools: BTreeMap<Address, Selection> = BTreeMap::new();
        let pool = Address::repeat_byte(0xaa);
        pools.insert(pool, swap);
        match pools.get(&pool) {
            Some(sel) if sel.is_swap => {}
            _ => {
                pools.insert(pool, liq);
            }
        }
        let kept = &pools[&pool];
        assert_eq!(kept.sqrt_price_x96, sqrt);
        assert_eq!(kept.amount0, amt0);
        assert_eq!(kept.amount1, amt1);
    }

    /// Lending topic constants match their event bindings.
    #[test]
    fn lending_topic_constants_match_event_bindings() {
        assert_eq!(
            TOPIC_LENDING_SUPPLY,
            lending_events::Supply::SIGNATURE_HASH,
            "lending Supply topic drifted from its binding"
        );
        assert_eq!(
            TOPIC_LENDING_WITHDRAW,
            lending_events::Withdraw::SIGNATURE_HASH,
            "lending Withdraw topic drifted from its binding"
        );
        assert_eq!(
            TOPIC_LENDING_BORROW,
            lending_events::Borrow::SIGNATURE_HASH,
            "lending Borrow topic drifted from its binding"
        );
        assert_eq!(
            TOPIC_LENDING_REPAY,
            lending_events::Repay::SIGNATURE_HASH,
            "lending Repay topic drifted from its binding"
        );
        assert_eq!(
            TOPIC_LENDING_SUPPLY_COLLATERAL,
            lending_events::SupplyCollateral::SIGNATURE_HASH,
            "lending SupplyCollateral topic drifted from its binding"
        );
        assert_eq!(
            TOPIC_LENDING_WITHDRAW_COLLATERAL,
            lending_events::WithdrawCollateral::SIGNATURE_HASH,
            "lending WithdrawCollateral topic drifted from its binding"
        );
    }

    /// Registry keys are stored and looked up by their computed pool id;
    /// replace semantics clear previous entries.
    #[test]
    fn rates_pool_registry_replace_and_lookup() {
        use alloy_primitives::keccak256;
        use alloy_sol_types::SolValue;

        let key = IV4Rates::PoolKey {
            currency0: Address::repeat_byte(0x00),
            currency1: Address::repeat_byte(0x11),
            fee: alloy_primitives::Uint::<24, 1>::try_from(3000u32).expect("fee fits"),
            tickSpacing: Signed::<24, 1>::try_from(60i32).expect("tick spacing fits"),
            hooks: Address::repeat_byte(0x00),
        };
        let id = pool_key_id(&key);
        let encoded = key.abi_encode();
        assert_eq!(id, keccak256(encoded.clone()), "id must be keccak256 of abi-encoded key");

        let registry = RatesPoolRegistry::new();
        assert!(!registry.contains_id(&id));

        let returned = registry.replace_keys(vec![key]);
        assert_eq!(returned, vec![id], "replace_keys returns computed ids in order");
        assert!(registry.contains_id(&id));

        let stored = registry.keys_for(&[id]);
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].abi_encode(), encoded, "stored key must round-trip");

        // Replace clears: an empty list disables tracking.
        assert!(registry.replace_keys(Vec::new()).is_empty());
        assert!(!registry.contains_id(&id));
        assert!(registry.keys_for(&[id]).is_empty());
    }

    /// Liquity `getRates` rows round-trip through the sol binding and map
    /// to decimal-string snapshot entries.
    #[test]
    fn liquity_get_rates_round_trip() {
        let outs = vec![ILiquityRates::Out {
            protocol: U256::from(2u8),
            col0: vec![U256::from(10u8), U256::from(11u8)],
            col1: vec![U256::from(20u8)],
            col2: Vec::new(),
            col3: vec![U256::from(40u8)],
            col4: Vec::new(),
            col5: vec![U256::from(60u8), U256::from(61u8)],
        }];

        let encoded = ILiquityRates::getRatesCall::abi_encode_returns(&outs);
        let decoded = ILiquityRates::getRatesCall::abi_decode_returns(&encoded)
            .expect("encoded returns must decode");
        assert_eq!(decoded.len(), 1);

        let entry = liquity_entry(&decoded[0]);
        assert_eq!(entry.protocol, "2");
        assert_eq!(entry.col0, vec!["10".to_string(), "11".to_string()]);
        assert!(entry.col2.is_empty());
        assert_eq!(entry.col5.len(), 2);

        // Snapshot JSON keeps camelCase field names.
        let json = serde_json::to_value(&entry).expect("entry serializes");
        assert_eq!(json["protocol"], serde_json::json!("2"));
        assert_eq!(json["col0"], serde_json::json!(["10", "11"]));
    }
}
