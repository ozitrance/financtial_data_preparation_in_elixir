defmodule ImbalanceBarsReduce do

  require Explorer.DataFrame # Needed so we can use lazy functions like DF.mutate_with

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  @input_csv_file "BTCUSDT-aggTrades-2024-09.csv"

  defp get_expected_imbalance(state, index, imbalance_series) do
    # Calculating the actual window and current alpha
    window = min(index + 1, state.expected_imbalance_window)
    alpha = 2 / (window + 1)

    imbalance_series
      # Our slicing operation
      |> Series.slice(max(0, index + 1 - state.expected_imbalance_window), window)
      # All of our calculate_ewma function is in this next row!
        # Series.ewm_mean is using the weighted version (same as we did in our NIF) by default
        # You can change that by setting the option [adjust: false]
      |> Series.ewm_mean([alpha: alpha])
      |> Series.last() # Getting last item of the list
  end

  defp get_expected_num_ticks(state, num_ticks_per_bar) do
    num_ticks_per_bar
      # Our slicing operation
      |> Series.tail(state.num_prev_bars_window)
      # All of our calculate_ewma function is in this next row!
        # Series.ewm_mean is using the weighted version (same as we did in our NIF) by default
        # You can change that by setting the option [adjust: false]
      |> Series.ewm_mean([alpha: state.num_prev_bars_alpha])
      |> Series.last() # Grabbing the last value
      # And clamping according to our constraints
      |> max(state.expected_num_ticks_constraints |> elem(0))
      |> min(state.expected_num_ticks_constraints |> elem(1))
  end

  defp update_first_expected_imbalance_if_needed(state, index, imbalance_series) do
    case state do
      # If state.expected_imbalance is a float it was already calculated so it's not the first bar!
      state when is_float(state.expected_imbalance) -> state
      # Other wise we check if we passed the initial state.expected_num_ticks to calculate expected_imbalance for the first time, and break out of warmup
      state when state.cum_ticks >= state.expected_num_ticks  ->
        %{
          state |
          expected_imbalance: get_expected_imbalance(state, index, imbalance_series),
        }
      # If we get here we're probably still in warmup
      _ -> state
      end
  end

  # Our functions arguments, starting with our imbalance_series and all the default values
  def compute_imbalance_bars(
    imbalance_series,
    num_prev_bars_window \\ 3,
    expected_imbalance_window \\ 1000,
    expected_num_ticks_init \\ 100,
    expected_num_ticks_constraints \\ {100, 1000}
    ) do

    # Creating our initial state
    initial_state = %{
      index: 0,
      cum_theta: 0,
      expected_imbalance: nil,
      expected_num_ticks: expected_num_ticks_init,
      num_ticks_per_bar: Series.from_list([]),
      cum_ticks: 0,
      current_bar_number: 0,
      bar_numbers: [] ,
      expected_imbalance_window: expected_imbalance_window,
      expected_num_ticks_constraints: expected_num_ticks_constraints,
      num_prev_bars_window: num_prev_bars_window,
      num_prev_bars_alpha: 2 / (num_prev_bars_window + 1)
    }

    # Creating a list from our series so we can enumerate over
    imbalance_series_list = imbalance_series |> Series.to_list

    # Starting our reduce operation, going through every row using our indexes array starting with initial_state
    final_state = Enum.reduce(imbalance_series_list, initial_state, fn imbalance, state ->

      # Getting the current index before we update it
      current_index = state.index

      # Updating Values On Each Cycle
      state = %{
        state |
        index: state.index + 1,
        cum_theta: state.cum_theta + imbalance,
        cum_ticks: state.cum_ticks + 1,
        bar_numbers: [state.current_bar_number | state.bar_numbers],
        } |> update_first_expected_imbalance_if_needed(current_index, imbalance_series) # This is where we break out of the 'warmup'

      # If still in 'warmup' threshold is nil, otherwise calculate the new threshold
      threshold = if is_nil(state.expected_imbalance), do: nil, else: state.expected_num_ticks * abs(state.expected_imbalance)

      # If we have a threshold (not in 'warmup') and condition met, we create a New Bar
      if !is_nil(threshold) and abs(state.cum_theta) > threshold do
        new_num_ticks_per_bar = Series.concat(state.num_ticks_per_bar, Series.from_list([state.cum_ticks]))
        new_expected_num_ticks = get_expected_num_ticks(state, new_num_ticks_per_bar)
        new_expected_imbalance = get_expected_imbalance(state, current_index, imbalance_series)

        # Resetting cumulative variables and incrementing our bar number
        %{state |
          cum_theta: 0,
          cum_ticks: 0,
          num_ticks_per_bar: new_num_ticks_per_bar,
          expected_num_ticks: new_expected_num_ticks,
          expected_imbalance: new_expected_imbalance,
          current_bar_number: state.current_bar_number + 1,
        }

      else
        state
      end

    end)

    # Reverse bars since we kept adding to the front since it's faster in Elixir
    Enum.reverse(final_state.bar_numbers)
  end

  def run() do
    df = DF.from_csv!(@input_csv_file, [lazy: true])
    |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
    |> DF.select(["timestamp", "price", "volume", "is_sell"])
    |> DF.mutate_with(&[
      dollar_value: Series.multiply(&1[:price], &1[:volume]),
      tick: 1.0])
    # Creating our imbalance series according to bar_type
    # and mutliplying by is_sell to add 'direction' (sell order values will become negative, i.e. 100 -> -100)
    |> DF.mutate_with(&[
      imbalance: Series.multiply(
        &1["tick"], Series.select(&1[:is_sell], -1.0, 1.0)
      )])
    |> DF.collect()

    # Running our Reduce operation
    bar_numbers = df["imbalance"]
      |> compute_imbalance_bars
      |> Series.from_list

    # Adding the result and summarizing as before
    df
      |> DF.put(:bar_number, bar_numbers)
      |> Utils.summarise_by_bar_number

  end


end
