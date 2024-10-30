
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
      |> Vl.encode(:x, field: "timestamp", title: "Date", type: :temporal) # timestamp on the x axis is a temporal type - represents datetime values
      |> Vl.encode(:y, field: "count", title: "Bar Count", type: :quantitative) # count is our data we want to plot on the y axis
      |> Vl.encode_field(:color, "series", type: :nominal) # differentiate the series column by color
      |> Vl.encode_field(:stroke_dash, "series", type: :nominal) # and then by stroke_dash size
      |> save_as_png("bar_count.png") # save the output as png file
  end

end
