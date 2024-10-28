defmodule TimeBars do
  require Explorer.DataFrame # Needed so we can use lazy functions like DF.mutate_with

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  @input_csv_file "BTCUSDT-aggTrades-2024-09.csv"

  def get_bars do
    # Getting the timeframe in milliseconds. Can also just use an int directly.
    timeframe_in_ms = ConvertDuration.to_milliseconds("1 day")

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
      # Divding our new series by the duration we want and casting back to int - which will remove any decimal point values
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
        date_time: ^timeframe_in_ms * Series.first(bar_number) + ^first_time_stamp, # Time of the bar. In bar_number series all rows are the same so first/last/min/max all would work.
        open: Series.first(price), # First price in the group - the open price
        high: Series.max(price), # Highest price in the group - the high price
        low: Series.min(price), # Lowest price in the group - the low price
        close: Series.last(price), # Last price in the group - the close price
        volume: Series.sum(volume), # First price in the group - the open price
        num_of_trades: Series.count(price), # First price in the group - the open price
        dollar_value: Series.sum(dollar_value) # First price in the group - the open price
      )
      # Getting rid of the bar_number column as it is really our row number now
      |> DF.discard("bar_number")



  end
end
