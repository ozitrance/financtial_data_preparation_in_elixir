defmodule Labeling do

  require Explorer.DataFrame

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  @lookback_window 100 # For our daily_return standard deviation ewma calculation
  @vertical_barrier_offset "4 hrs"

  def compute_tripple_barriers do
    # Getting dollar bars using functions from my previous article
    # Check it out for more info: https://medium.com/@yairoz/preparing-financial-time-series-for-ml-in-elixir-part-2-information-driven-bars-with-explorer-c59ccb996a68
    df = Bars.get_standard_bars("dollar")
    time_frame_in_ms = ConvertDuration.to_milliseconds("1 day") # Converting "1 day" to ms to use as window to calculate a daily_return Series

    # Getting previous_timestamps to calculate daily_return
    previous_timestamps_indices =
      df["timestamp"]
        |> Series.to_iovec
        # Using our NIF to search the closest timestamp of each value minus "1 day" (-time_frame_in_ms)
        # nil in 2nd argument means we only use 1 list to search
        |> FinancialDataPreparationNIF.searchsorted(-time_frame_in_ms, nil)
        |> Nx.from_binary({:s, 64})

    # Picking up previous prices from "close" serious
    previous_prices =
      df["close"]
        |> Series.to_tensor()
        |> Nx.indexed_put(Nx.tensor([0]), -1) # Masking first row since entries will be older than our earliest date and will be filtered
        |> Nx.take(previous_timestamps_indices) # Picking up prices from the "close" series using previous_timestamps_indices
        |> Series.from_tensor() # and creating a new series from it

    df =
      df
        # Adding our "previous_prices" to the our dataframe
        |> DF.put("previous_prices", previous_prices)
        # Filtering all the rows that picked up the first "close" price
        |> DF.filter(previous_prices >= 0)
        |> DF.mutate_with(&[
          # Calculating an exponantialy moving average for daily returns standard deviation
          # Pretty mouthful for only 3 rows with Explorer!
          daily_return:
            Series.divide(&1[:close], &1[:previous_prices])
              |> Series.subtract(1)
              |> Series.ewm_standard_deviation([alpha: 2 / (@lookback_window + 1), adjust: true])
        ])

    # Finally getting our trade entry points,
    # using our newly created daily_return series' mean as a threshold,
    # and our "close" prices as the input search series
    entry_events_indices = symmetric_cumulative_sum_filter(df["close"], df["daily_return"] |> Series.mean)

    # Adding Vertical Barriers for each entry using time offset (commented row below using bars offset)
    vertical_barriers_indices = vertical_barriers(entry_events_indices, df["timestamp"], @vertical_barrier_offset)
    # vertical_barriers_indices = vertical_barriers(entry_events_indices, df["timestamp"], 10, "bars")

    # Some values to play with - could be arguments for the function
    side = 1 # Direction of our trade: 1 for long, -1 for short
    tp = 0.5 # Profit taking multiple
    sl = 1 # Stop loss taking multiple

    # Adding those values as series to our dataframe
    # We can also us an actual series with different values, but we'll keep that part simple in this example.
    df = df |> DF.mutate(
      side: ^side,
      tp: ^tp,
      sl: ^sl
    )


    # Calculating Touches and Labeling each trade entry
    events_df =
      # Iterating over each trade entry event and its correspending vertical barrier
      Enum.zip(entry_events_indices |> Nx.to_list, vertical_barriers_indices |> Nx.to_list)
        # Using Flow to run them independently in parallel
        |> Flow.from_enumerable()
        # Mapping over each pair - this is the "meat" of this process
        |> Flow.map(fn {entry_event, vertical_barrier} ->

          # .... WE WILL FILL THIS PART IN THE NEXT STEP ....

          # Grabbing the range for the entry event until the vertical barrier
          sliced = DF.slice(df, entry_event..vertical_barrier)
          # Getting the price close price at our entry point
          previous_close = Series.first(sliced["close"])
          # And calculating returns adjusted to that price
          sliced = sliced
            |> DF.mutate_with(
              &[cumulative_returns:
                Series.divide(&1["close"], previous_close)
                  |> Series.subtract(1)
                  |> Series.multiply(&1["side"])
              ]
            )

          # Getting stop loss and profit taking multipliers value at our entry point
          first_tp = Series.first(sliced[:tp])
          first_sl = Series.first(sliced[:sl])

          # Getting daily_return value at our entry point
          first_daily_return = Series.first(sliced[:daily_return])

          # Calculating absolute values with our multiplier, and the daily return
          # These are our horizontal barriers
          local_tp = first_tp * first_daily_return
          local_sl = first_sl * first_daily_return * -1

          # Marking all points where stop loss and take profit values reached (horizontal barrier touches)
          tp_touches = Series.select(Series.greater(sliced[:cumulative_returns], local_tp), sliced[:timestamp], :infinity)
            |> Series.cast({:s, 64})
          sl_touches = Series.select(Series.less(sliced[:cumulative_returns], local_sl), sliced[:timestamp], :infinity)
            |> Series.cast({:s, 64})

          # Finding first touches (minimum timestamps)
          first_tp_touch = Series.min(tp_touches)
          first_sl_touch = Series.min(sl_touches)

          # Finding first touch overall, and assigning the label
          label = case {first_tp_touch, first_sl_touch} do
            # If no horizontal touches - our vertical barrier reached so we label as 0
            {nil, nil} -> 0
            # If we only touched the profit taking barrier we label it as on
            {_first_tp_touch, nil} -> 1
            # If we only touched the stop loss barrier we label it as on
            {nil, _first_sl_touch} -> -1
            # If we touched both the profit taking and stop loss barriers, we check which one was first and label accordingly
            {first_tp_touch, first_sl_touch} -> if first_tp_touch < first_sl_touch, do: 1, else: -1
          end
          # Our return list for each entry event
          [
            timestamp: Series.first(sliced[:timestamp]), # Entry point timestamp
            label: label, # The label
            vertical_barrier_timestamp: Series.last(sliced[:timestamp]), # Vertical Barrier - the last timestamp
            target: Series.first(sliced[:daily_return]), # The daily return we used to calculate our stop loss and profit taking values
            tp: Series.min(tp_touches), # Profit taking multiplier used
            sl: Series.min(sl_touches) # Stop loss multiplier used
          ]
        end)
        # Collecting all results back to a list (each event will return a list of [key: value] pairs)
        |> Enum.to_list()
        # Then creating a new Dataframe from all of our result lists - each event in a row
        |> DF.new


    ############ This part is mostly for Vega Lite charts ############

    # Stacking our vertical barrier timestamps, and first touches for each event side by side (just like in a table/df) using Nx so we can find the first/lowest one out of all
    stacked_tensors = Nx.stack([events_df["vertical_barrier_timestamp"] |> Series.cast({:f, 64}), events_df["tp"] |> Series.cast({:f, 64}) |> Series.fill_missing(:infinity), events_df["sl"] |> Series.cast({:f, 64}) |> Series.fill_missing(:infinity)], axis: 1)
    # Creating a tensor of the smallest timestamp (first) for each row out of those 3 series - first barrier touch for each event
    first_barrier_touch = Nx.reduce_min(stacked_tensors, axes: [1]) |> Series.from_tensor() |> Series.cast({:s, 64})
    # Getting the price at the first barrier touch
    prices_at_first_touch =
      DF.join(DF.new(%{timestamp: first_barrier_touch}), df, [on: ["timestamp"], how: :left])["close"] |> Series.to_tensor()
    # Prices at entry points
    prices_at_event = Nx.take(df["close"] |> Series.to_tensor(), entry_events_indices)
    # A series of return values for each event (how much we profited or lost)
    ret = Nx.subtract(prices_at_first_touch |> Nx.log(), prices_at_event |> Nx.log()) |> Nx.exp() |> Nx.subtract(1)
    # Updating our events_df with all the extra columns
    events_df = events_df
      |> DF.mutate([
        ret: ^ret |> Series.from_tensor(),
        prices_at_event: ^prices_at_event |> Series.from_tensor(),
        prices_at_first_touch: ^prices_at_first_touch |> Series.from_tensor(),
        first_barrier_touch: ^first_barrier_touch,
        vertical_barrier_timestamp: vertical_barrier_timestamp,
        tp: Series.cast(tp, {:naive_datetime, :millisecond}),
        sl: Series.cast(sl, {:naive_datetime, :millisecond}),
        event: true
      ])
    # And joining it to the original df
    df = df
      |> DF.join(events_df, [on: ["timestamp"], how: :left])

    # Getting Labels Frequencies
    events_df["label"] |> Series.frequencies |> DF.print()

    # Creating our Vega Lite Chart
    # vl_chart = VlHelper.get_final_vertical_barriers_chart(df, events_df)
    # VlHelper.save_as_png(vl_chart, "tripple_barrier.png")
  end



  def symmetric_cumulative_sum_filter(series, threshold) do

    series
      |> Series.log # Getting log values out of the series
      |> then(&Series.subtract(&1, Series.shift(&1, -1))) # Subtracting by prices of row before - like .diff in pandas
      |> Series.fill_missing(0.0)
      |> Series.to_iovec # Converting to_iovec - preventing copying the data before sending to the NIF
      |> FinancialDataPreparationNIF.symmetric_cumulative_sum_with_reset(threshold) # Using our NIF to calculate entry events
      |> Nx.from_binary({:s, 64}) # Back from the NIF to a list of timestamps

  end

  # Functin defintion with our defaults
  def vertical_barriers(entry_events_indices, timestamps, offset, duration_or_bars \\ "druation")

  # If we get text we convert to duration in integer and send it with this argument
  def vertical_barriers(entry_events_indices, timestamps, time_offset, "druation") when is_binary(time_offset) do
    time_offset = ConvertDuration.to_milliseconds(time_offset)
    vertical_barriers(entry_events_indices, timestamps, time_offset, "druation")
  end

  # Where we actually calculate the barriers when using duration as offset
  def vertical_barriers(entry_events_indices, timestamps, time_offset, "druation") when is_integer(time_offset) do

    timestamp_tensor = timestamps |> Series.to_tensor # Our timestamps series
    searchList = Nx.take(timestamp_tensor, entry_events_indices) |> Series.from_tensor # Entry points timestamps

    # Using our searchsorted NIF to find the closest timestamps (their indices) for each (entry_point + offset)
    searchList
      |> Series.to_iovec
      |> FinancialDataPreparationNIF.searchsorted(time_offset, timestamps |> Series.to_iovec)
      |> Nx.from_binary({:s, 64})

  end

  # Add Vertical Barriers by Bar Offset
  def vertical_barriers(entry_events_indices, _timestamp_tensor, bar_offset, "bars") when is_integer(bar_offset) do
      # Much simpler - just adding offset to entry event indices
      Nx.add(entry_events_indices, bar_offset)
  end

end
