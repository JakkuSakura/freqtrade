# State Sync & OMS Refactor Plan

## Objectives
- Centralize all exchange, wallet, and prediction refresh logic in a dedicated `DataSyncService`.
- Provide a thread-safe in-memory `StateStore` that reconciles live snapshots, strategy predictions, and local deltas.
- Replace ad-hoc reads (exchange, DB) with a uniform `StateReader` interface for the rest of the system.
- Split order/trade persistence into a lightweight historical store; introduce an in-process OMS that owns active orders and trades life cycle.
- Maintain parity with existing behaviour during migration, with clear toggles and roll-back paths.

## Target Architecture
1. **DataSyncService (`freqtrade/state/sync_service.py`)**
   - Schedules and deduplicates fetches from exchange APIs (balances, positions, tickers), DB snapshots, and strategy predictors.
   - Emits snapshot batches to the `StateStore` and marks refresh metadata (latency, source timestamp).
   - Acts as the only writer to the in-memory state.

2. **StateStore (`freqtrade/state/store.py`)**
   - Holds immutable snapshots (`WalletState`, `PositionState`, `TradeState`, `OrderState`, `ForecastState`).
   - Applies updates atomically via transaction context to avoid partial reads.
   - Offers cheap delta tracking for UI streaming and event dispatch.

3. **StateReader (`freqtrade/state/reader.py`)**
   - Thin facade that exposes typed query methods (e.g., `get_stake_balance()`, `iter_positions(filter=...)`).
   - Consumers (strategies, RPC, risk checks) depend on this interface instead of touching exchange/DB.

4. **StateEvents bus (`freqtrade/state/events.py`)**
   - Pub/Sub for snapshot updates so consumers can react (auto-refresh, risk triggers) without polling.

5. **Order Management Service (`freqtrade/oms/`)**
   - Owns creation, amendment, cancellation of active orders and trades.
   - Keeps live order/trade state in memory (backed by `StateStore`) and persists append-only history to the DB for analytics only.
   - Bridges between strategy intents and exchange execution while remaining source of truth for active positions.

6. **Historical Repository (`freqtrade/persistence/history_repository.py`)**
   - Persists finalized trades/orders to the existing DB schema (or a simplified projection) strictly for reporting/backtesting.
   - Provides replay APIs but never fed back into live decision loops.

## Migration Phases
1. **Phase 0 – Scaffold**
   - Add new `freqtrade/state/` and `freqtrade/oms/` packages with dataclasses, interfaces, and basic wiring.
   - Define DTOs for wallets, positions, orders, trades, forecasts.
   - Introduce feature flag `state_manager.enabled` to guard new flow. (Completed; flow now defaulted on.)

2. **Phase 1 – Dual Read**
   - Hook `DataSyncService` to refresh balances/positions alongside current `Wallets` implementation.
   - Add parity assertions/tests comparing `StateReader` output against existing wallet/position methods.

3. **Phase 2 – OMS Integration**
   - Route new orders/trade updates through OMS; keep DB writes for history via repository hooks.
   - Ensure legacy code path still runs for fallback (flag controlled).

4. **Phase 3 – Consumer Migration**
   - Move RPC endpoints, strategy helper methods, and risk management to consume `StateReader` exclusively.
   - Remove direct DB/exchange reads from those modules.

5. **Phase 4 – Cleanup**
   - Deprecate old wallet state handling, consolidate tests, and document new extension points.
   - Harden event bus / state store with stress tests and coverage for concurrency edge cases.

## Immediate Tasks (Sprint 1)
- Implement module skeletons (`DataSyncService`, `StateStore`, DTOs, events) with no external dependencies yet.
- Wire feature flag configuration entries and unit tests for basic store update semantics.
- Map current balance/position fetchers into `DataSyncService` under the flag, logging parity stats for manual validation.

## Risks & Mitigations
- **Partial Migration Complexity**: Use feature flags and shadow-mode parity checks before flipping consumers.
- **Concurrency Bugs**: Enforce atomic updates via context manager and add extensive tests for read-after-write ordering.
- **Performance Regressions**: Benchmark current vs new read paths; keep snapshots lightweight and avoid deep copies where unnecessary.

## Progress
- ✅ State modules, event bus, and OMS skeleton created; state manager now enabled by default with the legacy flag removed.
- ✅ Wallet/position snapshots feed the state store; DataSync producers cover wallets, trades, and orders.
- ✅ Added parity tests to ensure state-backed wallets/positions match legacy values.
- ✅ RPC balance/position/status endpoints shadow state manager output with parity coverage.
- ✅ OrderManagementService now maintains live orders/trades and publishes them into the state store.
- ✅ RPC trade/order endpoints consume the state cache with parity tests guarding legacy fallbacks.
- ✅ Historical repository captures closed trades/orders in dedicated append-only tables.
