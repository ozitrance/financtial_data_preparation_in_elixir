defmodule Utils do
  require Explorer.DataFrame
  alias Explorer.Series
  alias Explorer.DataFrame, as: DF

  def get_threshold(df, column, divisor \\ 50) do
    mean = df[column]
      |> Series.mean()

    mean / divisor
  end
  def summarise_by_bar_number(df) do
    df
      |> DF.group_by(:bar_number)
      # Summarizing the results to create a new DF with our bars - each group will be a new row in our new DF
      |> DF.summarise(
          date_time: Series.first(timestamp), # Time of the first trade in the bar.
          open: Series.first(price), # First price in the group - the open price
          high: Series.max(price), # Highest price in the group - the high price
          low: Series.min(price), # Lowest price in the group - the low price
          close: Series.last(price), # Last price in the group - the close price
          volume: Series.sum(volume), # Volume as units of the currecy
          buy_volume: Series.sum(Series.select(is_sell, 0, volume)), # Volume of only the market buy orders
          dollar_value: Series.sum(dollar_value), # Volume in Dollar value
          num_of_trades: Series.count(price) # Number of trades in the bar
      )
      # Getting rid of the bar_number column as it is really our row number now
      |> DF.discard("bar_number")
  end


end

defmodule ConvertDuration do
  @moduledoc """
  Module to convert seconds to compound time, or cron duration notation
  to seconds
  """
  @second 1
  @minute 60
  @hour @minute * 60
  @day @hour * 24
  @week @day * 7
  @divisor [@week, @day, @hour, @minute, 1]

  @doc ~S"""
  Convert a set number of seconds to a compound time.
  Taken from https://rosettacode.org/wiki/Convert_seconds_to_compound_duration#Elixir

  ## Example

      iex> TimeConvert.to_compound(336)
      "5 min, 36 sec"

      iex> TimeConvert.to_compound(6358794)
      "10 wk, 3 d, 14 hr, 19 min, 54 sec"
  """
  @spec to_compound(pos_integer()) :: binary()
  def to_compound(sec) do
    {_, [s, m, h, d, w]} =
      Enum.reduce(@divisor, {sec, []}, fn divisor, {n, acc} ->
        {rem(n, divisor), [div(n, divisor) | acc]}
      end)

    ["#{w} wk", "#{d} d", "#{h} hr", "#{m} min", "#{s} sec"]
    |> Enum.reject(fn str -> String.starts_with?(str, "0") end)
    |> Enum.join(", ")
  end

  @doc ~S"""
  Convert a specially crafted string to seconds. Inspired by
  https://github.com/henrypoydar/chronic_duration/blob/master/lib/chronic_duration.rb

  ## Examples

      iex> TimeConvert.to_seconds("5 min 36 sec")
      336

      iex> TimeConvert.to_seconds("2 hours 10min 36 secs")
      7836

      iex> TimeConvert.to_seconds("2h10m36s")
      7836

      iex> TimeConvert.to_seconds("7d")
      604800
  """
  @spec to_seconds(binary()) :: pos_integer()
  def to_seconds(cron_string) do
    cron_string
    |> cleanup()
    |> calculate_from_words()
  end

  @spec to_milliseconds(binary()) :: pos_integer()
  def to_milliseconds(cron_string) do
    cron_string
    |> to_seconds()
    |> then(& &1 * 1000)

  end

  ###################
  # Private functions
  ###################
  defp cleanup(string) do
    string
    |> String.downcase()
    |> String.replace(number_matcher(), " \\0 ")
    |> String.trim(" ")
    |> filter_through_white_list()
  end

  defp calculate_from_words(string) do
    string
    |> Enum.with_index()
    |> Enum.reduce(0, fn {value, index}, acc ->
      if Regex.match?(number_matcher(), value) do
        acc +
          String.to_integer(value) *
            (string |> Enum.at(index + 1) |> duration_units_seconds_multiplier())
      else
        acc
      end
    end)
  end

  defp duration_units_seconds_multiplier(unit) do
    case unit do
      "years" -> 31_557_600
      "months" -> @day * 30
      "weeks" -> @week
      "days" -> @day
      "hours" -> @hour
      "minutes" -> @minute
      "seconds" -> @second
      _ -> 0
    end
  end

  defp filter_through_white_list(string) do
    string
    |> String.split(" ")
    |> Enum.map(fn sub ->
      if Regex.match?(number_matcher(), sub) do
        String.trim(sub)
      else
        if mappings()[sub] in ~w(seconds minutes hours days weeks months years) do
          String.trim(mappings()[sub])
        end
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp number_matcher do
    Regex.compile!("[0-9]*\\.?[0-9]+")
  end

  defp mappings do
    %{
      "seconds" => "seconds",
      "second" => "seconds",
      "secs" => "seconds",
      "sec" => "seconds",
      "s" => "seconds",
      "minutes" => "minutes",
      "minute" => "minutes",
      "mins" => "minutes",
      "min" => "minutes",
      "m" => "minutes",
      "hours" => "hours",
      "hour" => "hours",
      "hrs" => "hours",
      "hr" => "hours",
      "h" => "hours",
      "days" => "days",
      "day" => "days",
      "dy" => "days",
      "d" => "days",
      "weeks" => "weeks",
      "week" => "weeks",
      "wks" => "weeks",
      "wk" => "weeks",
      "w" => "weeks",
      "months" => "months",
      "mo" => "months",
      "mos" => "months",
      "month" => "months",
      "years" => "years",
      "year" => "years",
      "yrs" => "years",
      "yr" => "years",
      "y" => "years"
    }
  end
end

defmodule VlHelper do
  require Explorer.DataFrame
  require Explorer.Series
  alias Explorer.Series
  alias VegaLite, as: Vl
  alias Explorer.DataFrame, as: DF

  # This function expects time column to be called "date_time" and price column to be called "price".
  def get_bars_plot(df, title \\ "", height \\ 600, width \\ 800) do

    # Initiating the VegaLite instance with width and height
    Vl.new(width: width, height: height)
      |> Vl.data_from_values(DF.to_rows(df)) # Converting the DF to rows
      # Encoding our x/time axis. :temporal tells VL this data contains time units
      |> Vl.encode_field(:x, "date_time", title: title, type: :temporal, format: "%m/%d", labelAngle: -45)
      # Encoding the y/price axis. :quantitative tells VL this data is quantitative in nature. scale: [zero: false] tells VL we don't want to start this axis from 0 (our prices are in the ~ 60,000)
      |> Vl.encode(:y, field: "close", title: "Price", type: :quantitative, scale: [zero: false])
      # Encoding the colors for our bars, if price closes above the open price it's a green bar, otherwise red
      |> Vl.encode(:color, condition: [test: "datum.open < datum.close", value: "green"], value: "red")
      # Adding the bars layers
      |> Vl.layers([
        # The bodies ():bar type)
        Vl.new()
        |> Vl.mark(:bar)
        |> Vl.encode_field(:y, "open")
        |> Vl.encode_field(:y2, "close"),
        # The wicks (:rule type)
        Vl.new()
        |> Vl.mark(:rule)
        |> Vl.encode_field(:y, "low")
        |> Vl.encode_field(:y2, "high"),
      ])
  end

  def save_as_png(vl, path) do
    # Saving to png - Vl returns binary data which we just save to file with Exlixir's File module
    png_binary = Vl.Export.to_png(vl)
    File.write!(path, png_binary)
  end

  defp truncate_timestamp(series) do
    Series.cast(series, {:naive_datetime, :millisecond}) |> Series.transform(&NaiveDateTime.beginning_of_day/1)
  end
  # Expecting series_array to contain 3 timestamp series in this order: tick, volume, dollar
  def get_bar_count_plot(series_array, height \\ 600, width \\ 800) do

    # Apparently Enum cannot enumerate over a list of series so we do it manually...
    # Truncating - aligning timestamps to beginning of day so we can group by day
    tick_bars_series = series_array |> elem(0) |> truncate_timestamp
    volume_bars_series = series_array |> elem(1) |> truncate_timestamp
    dollar_bars_series = series_array |> elem(2) |> truncate_timestamp

    # Creating a DF from each series with another column to identify the series before we merge them
    tick_bars_df = DF.new([timestamp: tick_bars_series]) |> DF.mutate([series: "Tick Bars"])
    volume_bars_df = DF.new([timestamp: volume_bars_series]) |> DF.mutate([series: "Volume Bars"])
    dollar_bars_df = DF.new([timestamp: dollar_bars_series]) |> DF.mutate([series: "Dollar Bars"])

    # Merging our 3 DFs
    df = DF.concat_rows([tick_bars_df, volume_bars_df, dollar_bars_df])
      # Grouping by timestamp (day in our case), and series - so each series gets its own row for each day
      |> DF.group_by(["timestamp", "series"])
      # Counting the number of rows for each group / day in our case
      |> DF.summarise(count: count(series))

    # Creating new VegaLite Plot
    Vl.new(width: width, height: height)
      |> Vl.data_from_values(DF.to_rows(df))
      |> Vl.mark(:line) # Telling Vl we want a line plot
      |> Vl.encode_field(:x, "timestamp", type: :temporal) # timestamp on the x axis is a temporal type - represents datetime values
      |> Vl.encode_field(:y, "count", type: :quantitative) # count is our data we want to plot on the y axis
      |> Vl.encode_field(:color, "series", type: :nominal) # differentiate the series column by color
      |> Vl.encode_field(:stroke_dash, "series", type: :nominal) # and then by stroke_dash size
      |> save_as_png("bar_count.png") # save the output as png file
  end

end
