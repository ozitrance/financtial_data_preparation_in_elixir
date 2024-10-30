defmodule StandardBars do
  require Explorer.DataFrame # Needed so we can use lazy functions like DF.mutate_with

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  @input_csv_file "BTCUSDT-aggTrades-2024-09.csv"

  def get_time_bars(timeframe \\ "1 day") do
    # Getting the timeframe in milliseconds. Can also just use an int directly.
    timeframe_in_ms = ConvertDuration.to_milliseconds(timeframe)

    # Loading the file in Lazy mode
    df = DF.from_csv!(@input_csv_file, [lazy: true])
      # Renaming some columns for simplicity. is_buyer_maker tells us the direction: if True, seller is the "taker" which makes this trade a sell.
      |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
      # Selecting only the columns we care about
      |> DF.select(["timestamp", "price", "volume", "is_sell"])
      # Adding some extra data: a column for dollar_value = price * volume
      |> DF.mutate_with(&[dollar_value: Series.multiply(&1[:price], &1[:volume])])
      # Assigning bar numbers to each row
      |> DF.mutate_with(fn df ->
        # Getting the first timestamp for first bar
        first_time_stamp = df["timestamp"] |> Series.first
        # Creating a series for time elapsed_time_since_first_timestamp (current_trade_timestamp - first_timestamp)
        elapsed_ms = df["timestamp"] |> Series.subtract(first_time_stamp)
        # Dividing our new series by the duration we want and casting back to int - which will remove any decimal point values
        bar_numbers = elapsed_ms |> Series.divide(timeframe_in_ms) |> Series.cast({:s, 64})
        # And then returning the bars, which will add them as a series to out Dataframe
        [bar_number: bar_numbers]
      end)
      # Converting out Dataframe from Lazy to Eager - calculating our code so far
      |> DF.collect()

    # First timestamp of the dataframe - so we can use it later
    first_time_stamp = df["timestamp"] |> Series.first

    # Grouping our data frame by the bar numbers column we created
    df |> DF.group_by(:bar_number)
      # Summarizing the results to create a new DF with our bars - each group will be a new row in our new DF
      |> DF.summarise(
          # Time of the bar. We calculate it according to the timeframe and first_time_stamp.
          # In this case, bar_number series all rows are the same so first/last/min/max all would work.
          date_time: ^timeframe_in_ms * Series.first(bar_number) + ^first_time_stamp,
          open: Series.first(price), # First price in the group - the open price
          high: Series.max(price), # Highest price in the group - the high price
          low: Series.min(price), # Lowest price in the group - the low price
          close: Series.last(price), # Last price in the group - the close price
          volume: Series.sum(volume), # First price in the group - the open price
          buy_volume: Series.sum(Series.select(is_sell, 0, volume)), # Volume of only the market buy orders
          num_of_trades: Series.count(price), # First price in the group - the open price
          dollar_value: Series.sum(dollar_value) # First price in the group - the open price
      )
      # Getting rid of the bar_number column as it is really our row number now
      |> DF.discard("bar_number")

  end

  def get_tick_bars_vectorized do
    # Please note this implementation is not accurate
    threshold = DF.from_csv!("BTCUSDT-1d-2024-08.csv")
      |> Utils.get_threshold("count")

    DF.from_csv!(@input_csv_file, [lazy: true])
      |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
      |> DF.select(["timestamp", "price", "volume", "is_sell"])
      |> DF.mutate_with(&[dollar_value: Series.multiply(&1[:price], &1[:volume])])
      # Until here everything is the same

      # Now we create an index column, which is really a cumulative_sum for trades (each row is a trade which is +1 in a cum_sum context)
      # Adding 1 so first row starts at 1
      |> DF.mutate_with(&[index: Series.row_index(&1[:timestamp]) |> Series.add(1)])
      # Dividing our index by the threshold and casting to int to get rid of decimal point values
      |> DF.mutate_with(&[bar_number: Series.divide(&1[:index], threshold) |> Series.cast({:s, 64})])
      # And calculating all our lazy operations
      |> DF.collect()
      # From here it's the same as before
      |> Utils.summarise_by_bar_number

  end

  def get_standard_bars_reduce(bar_type) do
    # Load previous month df to get a threshold number
    previous_month_df = DF.from_csv!("BTCUSDT-1d-2024-08.csv")
    # Just chaning type "dollar" to "dollar_value" to keep in line with previous examples, while keeping "bar_type" options short
    bar_type = if bar_type == "dollar", do: "dollar_value", else: bar_type

    # Getting the threshold according to bar_type and raising an error if bar_type is not what we expect
    threshold = case bar_type do
      "tick" -> Utils.get_threshold(previous_month_df, "count")
      "volume" -> Utils.get_threshold(previous_month_df, "volume")
      "dollar_value" -> Utils.get_threshold(previous_month_df, "quote_volume")
      _ -> raise("Bar type can only be: tick, volume or dollar")
    end


    df = DF.from_csv!(@input_csv_file, [lazy: true])
      |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
      |> DF.select(["timestamp", "price", "volume", "is_sell"])
      |> DF.mutate_with(&[
          dollar_value: Series.multiply(&1[:price], &1[:volume]),
          # Adding a 'dummy' column with only Ones (1) to count trades/ticks
          tick: 1.0])
      |> DF.collect()
      # Until here everything is more or less the same

    # Let's calculate the bars. We start with the column holding the data we need.
    bars = df[bar_type]
      # Converting to list so we can enumerate over it
      |> Series.to_list()
      # Using reduce we keep tracking of the current_bar (starting with 1), cum_value (starting with 0) and our bar_number array (starting empty: [])
      |> Enum.reduce({1, 0, []}, fn value, {current_bar, cum_value, bars_array} ->
        # For each row we add the current bar to our bars array - adding to front to optimize speed
        new_bars_array = [ current_bar | bars_array ]
        # Updating cumulative_value
        new_cum_value = cum_value + value

        # If new_cum_value is greater than our threshold we increment our bar number and reset the cum_values counter
        if (new_cum_value >= threshold) do
          {current_bar + 1, 0, new_bars_array}
        # Otherwise we return the updated new_cum_value and new_bars_array without resetting or incrementing
        else
          {current_bar, new_cum_value, new_bars_array}
        end
      end)
      # Then we choose the 3rd element of our tuple (the bars_array)
      |> elem(2)
      # And reversing the array since we were adding new values to the front (it's much faster than pushing to the back in Elixir)
      |> Enum.reverse

    df
      # Now we can create a new series from our bars_array and add it to our DF
      |> DF.put(:bar_number, Series.from_list(bars))
      # And summarise as before
      |> Utils.summarise_by_bar_number
  end


  def get_standard_bars_c_nif(bar_type) do
    # Load previous month df to get a threshold number
    previous_month_df = DF.from_csv!("BTCUSDT-1d-2024-08.csv")
    # Just chaning type "dollar" to "dollar_value" to keep in line with previous examples, while keeping "bar_type" options short
    bar_type = if bar_type == "dollar", do: "dollar_value", else: bar_type

    # Getting the threshold according to bar_type and raising an error if bar_type is not what we expect
    threshold = case bar_type do
      "tick" -> Utils.get_threshold(previous_month_df, "count")
      "volume" -> Utils.get_threshold(previous_month_df, "volume")
      "dollar_value" -> Utils.get_threshold(previous_month_df, "quote_volume")
      _ -> raise("Bar type can only be: tick, volume or dollar")
    end


    df = DF.from_csv!(@input_csv_file, [lazy: true])
      |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
      |> DF.select(["timestamp", "price", "volume", "is_sell"])
      |> DF.mutate_with(&[
          dollar_value: Series.multiply(&1[:price], &1[:volume]),
          # Adding a 'dummy' column with only Ones (1) to count trades/ticks
          tick: 1.0])
      |> DF.collect()
      # Until here everything is more or less the same

    # Here we convert the series with our data to iovec type, and sending the data together with our threshold to our NIF
    bars = FinancialDataPreparationNIF.cumulative_sum_with_reset(Series.to_iovec(df[bar_type]), threshold)
        # This time we get binary data back (signed integer - the bar numbers) so we convert it back to a Series
        |> Series.from_binary({:s, 64})
    df
      # Now we can add our already created series to our DF
      |> DF.put(:bar_number, bars)
      # And summarise as before
      |> Utils.summarise_by_bar_number
  end

  def get_bar_count_plot do
    tick_bars = get_standard_bars_c_nif("tick")
    volume_bars = get_standard_bars_c_nif("volume")
    dollar_bars = get_standard_bars_c_nif("dollar")


    VlHelper.get_bar_count_plot({tick_bars["date_time"], volume_bars["date_time"], dollar_bars["date_time"]})

  end
end
