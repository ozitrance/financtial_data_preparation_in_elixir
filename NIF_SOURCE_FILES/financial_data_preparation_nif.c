
# include "financial_data_preparation_nif.h"
// Here we define the functions for this NIF. For now we only have 1 but we can have more
static ErlNifFunc nif_funcs[] = {
    // ERL_NIF_DIRTY_JOB_CPU_BOUND is needed since this function takes more than 1 ms, according to Erlang's docs, so we don't block the main VM thread
    {"cumulative_sum_with_reset", 2, cumulative_sum_with_reset, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"compute_imbalance_bars", 6, compute_imbalance_bars, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"compute_run_bars", 6, compute_run_bars, ERL_NIF_DIRTY_JOB_CPU_BOUND},
};

// Naming our NIF and setting our functions. 
// All the NULL values responsible for life cycle of our NIF, which we don't really use since our NIF works in sync and returns immediately
ERL_NIF_INIT(Elixir.FinancialDataPreparationNIF, nif_funcs, NULL, NULL, NULL, NULL)
