defmodule Benchmarks.StandardBars do

  def run() do
    Benchee.run(
      %{
        # "time_bars" => fn -> StandardBars.get_time_bars end,
        # "tick_bars_vectorized (result not accurate)" => fn -> StandardBars.get_tick_bars_vectorized end,
        "tick_bars_reduce" => fn -> StandardBars.get_standard_bars_reduce("tick") end,
        "tick_bars_c_nif" => fn -> StandardBars.get_standard_bars_c_nif("tick") end,
        "volume_bars_reduce" => fn -> StandardBars.get_standard_bars_reduce("volume") end,
        "volume_bars_c_nif" => fn -> StandardBars.get_standard_bars_c_nif("volume") end,
        "dollar_bars_reduce" => fn -> StandardBars.get_standard_bars_reduce("dollar") end,
        "dollar_bars_c_nif" => fn -> StandardBars.get_standard_bars_c_nif("dollar") end,
      },
      time: 30
    )


  end


end
