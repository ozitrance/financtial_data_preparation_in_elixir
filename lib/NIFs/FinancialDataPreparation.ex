defmodule FinancialDataPreparationNIF do
  @on_load :load_nif

  def load_nif do
    nif_file = :filename.join(:code.priv_dir(:financial_data_preparation), "financial_data_preparation")
    :erlang.load_nif(nif_file, 0)
  end

  def cumulative_sum_with_reset(_values, _threshold) do
    :erlang.nif_error(:not_loaded)
  end
end
