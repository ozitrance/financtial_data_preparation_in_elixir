#ifndef FINANCIAL_DATA_PREPARATION_NIF_H
#define FINANCIAL_DATA_PREPARATION_NIF_H

#include <erl_nif.h>

ERL_NIF_TERM cumulative_sum_with_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM compute_imbalance_bars(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM compute_run_bars(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif 