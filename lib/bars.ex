defmodule Bars do
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

  def get_standard_bars(bar_type) do
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

  def get_information_driven_bars(imbalance_or_run_bars, bar_type, ema_bars \\ 1) do

    # Creating anonymous function with the correct C NIF function, whether imbalance or run bars were requested
    computing_function = case imbalance_or_run_bars do
      "imbalance" -> &FinancialDataPreparationNIF.compute_imbalance_bars/6
      "run" -> &FinancialDataPreparationNIF.compute_run_bars/6
      _ -> raise("First argument can only be: imbalance or run")
    end

    # Just chaning type "dollar" to "dollar_value" to keep in line with previous examples, while keeping "bar_type" options short
    bar_type = if bar_type == "dollar", do: "dollar_value", else: bar_type
    # Making sure 2nd argument is what we expect
    if (bar_type !== "tick" && bar_type !== "volume" && bar_type !== "dollar_value"), do: raise("Second argument can only be: tick, volume or dollar")

    # A few default values - can be used as function arguments in the future
    num_prev_bars_window = 3 # Number of previous bars window we will use to slice array for calculating expected_num_ticks
    expected_imbalance_window = 1000.0 # Number of previous bars window we will use to slice array for calculating expected_imbalance
    exp_num_ticks = 100.0 # Initial value for calculating expected_num_ticks - for the warmup period
    constraints_tuple = {100.0, 1000.0} # Constraints for expected_imbalance values - preventing values from grow too much
    # ema_bars = 1 # Boolean value - should we calculate exp_num_ticks using EWMA or use Constant values every new bar

    df = DF.from_csv!(@input_csv_file, [lazy: true])
      |> DF.rename(transact_time: "timestamp", quantity: "volume", is_buyer_maker: "is_sell")
      |> DF.select(["timestamp", "price", "volume", "is_sell"])
      |> DF.mutate_with(&[
        dollar_value: Series.multiply(&1[:price], &1[:volume]),
        tick: 1.0])
      # Creating our "imbalance" series according to bar_type
      # and mutliplying by is_sell to add 'direction' (sell order values will become negative, i.e. 100 -> -100)
      |> DF.mutate_with(&[
        imbalance: Series.multiply(
          &1[bar_type], Series.select(&1[:is_sell], -1.0, 1.0)
        )])
      |> DF.collect()

    # Running our NIF Function
    bar_numbers = df["imbalance"]
      |> Series.to_iovec()
      |> computing_function.(
          num_prev_bars_window,
          expected_imbalance_window,
          exp_num_ticks,
          constraints_tuple,
          ema_bars
        )

    # Adding the result and summarizing as before
    df
      |> DF.put(:bar_number, Series.from_binary(bar_numbers, {:s, 64}))
      |> Utils.summarise_by_bar_number

  end

  def get_bar_count_plot do

    # VlHelper.get_bar_count_plot([
    #   {get_standard_bars("tick")["date_time"], "Tick Bars"},
    #   {get_standard_bars("volume")["date_time"], "Volume Bars"},
    #   {get_standard_bars("dollar")["date_time"], "Dollar Bars"}
    # ])

    VlHelper.get_bar_count_plot([
      {get_standard_bars("dollar")["date_time"], "Standard Dollar Bars"},
      {get_information_driven_bars("imbalance","dollar", 1)["date_time"], "EMA Imbalance Dollar Bars"},
      {get_information_driven_bars("imbalance","dollar", 0)["date_time"], "Constant Imbalance Dollar Bars"},
      {get_information_driven_bars("run","dollar", 1)["date_time"], "EMA Run Dollar Bars"},
      {get_information_driven_bars("run","dollar", 0)["date_time"], "Constant Run Dollar Bars"},
    ])

  end
end
