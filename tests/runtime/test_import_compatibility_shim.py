from __future__ import annotations

import warnings


def test_nested_runtime_modules_share_identity_across_import_sites() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import core_runtime.backtest.engine.strategy_runner as old_strategy_runner
        import core_runtime.strategies.debug_strategy as old_debug_strategy

    import core_runtime.backtest.engine.strategy_runner as new_strategy_runner
    import core_runtime.strategies.debug_strategy as new_debug_strategy

    assert old_strategy_runner is new_strategy_runner
    assert old_debug_strategy is new_debug_strategy


def test_runtime_symbols_share_identity_across_import_sites() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from core_runtime.backtest.engine.strategy_runner import HftStrategyRunner as OldRunner
        from core_runtime.strategies.debug_strategy import DebugStrategyV1 as OldStrategy

    from core_runtime.backtest.engine.strategy_runner import HftStrategyRunner as NewRunner
    from core_runtime.strategies.debug_strategy import DebugStrategyV1 as NewStrategy

    assert OldRunner is NewRunner
    assert OldStrategy is NewStrategy
