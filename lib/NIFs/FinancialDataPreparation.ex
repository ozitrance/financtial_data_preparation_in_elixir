defmodule FinancialDataPreparationNIF do
  @on_load :load_nif

  def load_nif do
    nif_file = :filename.join(:code.priv_dir(:financial_data_preparation), "financial_data_preparation")
    :erlang.load_nif(nif_file, 0)
  end

  def cumulative_sum_with_reset(_values, _threshold) do
    :erlang.nif_error(:not_loaded)
  end

  def compute_imbalance_bars(_values, _num_prev_bars, _expected_imbalance_window, _exp_num_ticks, _constraints_tuple, _ema_bars) do
    :erlang.nif_error(:not_loaded)
  end

  def compute_run_bars(_values, _num_prev_bars, _expected_imbalance_window, _exp_num_ticks, _constraints_tuple, _ema_bars) do
    :erlang.nif_error(:not_loaded)
  end

  def symmetric_cumulative_sum_with_reset(_values, _threshold) do
    :erlang.nif_error(:not_loaded)
  end

  def searchsorted(_needles, _offset, _haystack?) do
    :erlang.nif_error(:not_loaded)
  end

end
