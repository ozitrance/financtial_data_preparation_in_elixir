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
