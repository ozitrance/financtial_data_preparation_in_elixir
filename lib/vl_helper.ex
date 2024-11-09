
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
      |> Vl.encode_field(:x, "timestamp", title: title, type: :temporal, format: "%m/%d", labelAngle: -45)
      # Encoding the y/price axis. :quantitative tells VL this data is quantitative in nature. scale: [zero: false] tells VL we don't want to start this axis from 0 (our prices are in the ~ 60,000)
      |> Vl.encode(:y, field: "close", title: "Price", type: :quantitative, scale: [zero: false])
      # Encoding the colors for our bars, if price closes above the open price it's a green bar, otherwise red
      |> Vl.encode(:color, condition: [test: "datum.open < datum.close", value: "green"], value: "red")
      # Adding the bars layers
      |> Vl.layers([
        # The bodies (:bar type)
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

# This function accepts a list of tuples, each with 2 elements for {date_time_series, title}
  def get_bar_count_plot(tuples_array, height \\ 600, width \\ 800) do

    # Mapping our tuples to a list with Dataframes, after truncating our timestamps and adding a title column
    dfs_array = tuples_array
      |> Enum.map(fn tuple_item ->
        {series, title} = tuple_item
        series = truncate_timestamp(series)
        DF.new([timestamp: series]) |> DF.mutate([series: ^title])
      end)

    # Merging our DFs
    merged_df = DF.concat_rows(dfs_array)
      # Grouping by timestamp (day in our case), and series - so each series gets its own row for each day
      |> DF.group_by(["timestamp", "series"])
      # Counting the number of rows for each group / day in our case
      |> DF.summarise(count: count(series))

    # Creating new VegaLite Plot
    Vl.new(width: width, height: height)
      |> Vl.data_from_values(DF.to_rows(merged_df))
      |> Vl.mark(:line) # Telling Vl we want a line plot
      |> Vl.encode(:x, field: "timestamp", title: "Date", type: :temporal) # timestamp on the x axis is a temporal type - represents datetime values
      |> Vl.encode(:y, field: "count", title: "Bar Count", type: :quantitative) # count is our data we want to plot on the y axis
      |> Vl.encode_field(:color, "series", type: :nominal) # differentiate the series column by color
      |> Vl.encode_field(:stroke_dash, "series", type: :nominal) # and then by stroke_dash size
      |> save_as_png("bar_count_information_driven.png") # save the output as png file

  end

  def get_line_chart(df) do
    Vl.new(width: 800, height: 800)
      |> Vl.data_from_values(DF.to_rows(df))
      |> Vl.mark(:line, color: "blue", tooltip: true)
      |> Vl.encode_field(:x, "timestamp", type: :temporal, title: "Date")
      |> Vl.encode(:y, field: "close", type: :quantitative, title: "Price ($)", scale: [zero: false])
  end

  def get_vertical_barriers_only_chart(events_df) do
    Vl.new()
      |> Vl.data_from_values(DF.to_rows(events_df))
      |> Vl.mark(:rule, color: "red", strokeDash: [4, 4], size: 2)
      |> Vl.encode_field(:x, "timestamp", type: :temporal)
  end

  def get_final_vertical_barriers_chart(df, events_df) do
    df = df
      # Marking first barrier touch rows with true (events_df["event"])
      |> DF.join(DF.new(%{timestamp: events_df["first_barrier_touch"], first_touch: events_df["event"]}), [on: ["timestamp"], how: :left])
      # Marking vertical_barrier rows with true (events_df["event"])
      |> DF.join(DF.new(%{timestamp: events_df["vertical_barrier_timestamp"], vertical_barrier: events_df["event"]}), [on: ["timestamp"], how: :left])
      # Creating a series for the profit taking barriers - filling values until the next event
      |> DF.mutate(tp_barrier: Series.add(close, Series.multiply(close, target) |> Series.multiply(tp) |> Series.multiply(side)) |> Series.fill_missing(:forward))
      # Creating a series for the stop loss barriers - filling values until the next event
      |> DF.mutate(sl_barrier: Series.subtract(close, Series.multiply(close, target) |> Series.multiply(sl) |> Series.multiply(side)) |> Series.fill_missing(:forward))

    # Finally our Vega Lite chart
    Vl.new(width: 1200, height: 800)
      |> Vl.data_from_values(df
        |> DF.slice(390..420)
        )
      |> Vl.layers([
        # Line Chart for Prices
        Vl.new()
          |> Vl.mark(:line, color: "blue", tooltip: true)
          |> Vl.encode_field(:x, "timestamp", type: :temporal, title: "September 2024")
          |> Vl.encode_field(:y, "close", type: :quantitative, title: "BTC Price ($)", scale: [zero: false]),
        # Marking the Profit Taking Barrier
        Vl.new()
          |> Vl.mark(:line, color: "green", tooltip: true)
          |> Vl.encode_field(:x, "timestamp", type: :temporal)
          |> Vl.encode_field(:y, "tp_barrier", type: :quantitative, scale: [zero: false]),
        # Marking the Stop Loss Barrier
        Vl.new()
          |> Vl.mark(:line, color: "red", tooltip: true)
          |> Vl.encode_field(:x, "timestamp", type: :temporal)
          |> Vl.encode_field(:y, "sl_barrier", type: :quantitative, scale: [zero: false]),
        # Marking Entry Points
        Vl.new()
          |> Vl.mark(:rule, color: "black", size: 4)
          |> Vl.transform(filter: "datum.event === true") # Only include events where `event` is true
          |> Vl.encode_field(:x, "timestamp", type: :temporal),
        # Marking Vertical Barriers
        Vl.new()
          |> Vl.mark(:rule, color: "brown", stroke_dash: [4, 4], size: 2)
          |> Vl.transform(filter: "datum.vertical_barrier === true") # Only include events where `event` is true
          |> Vl.encode_field(:x, "timestamp", type: :temporal),
        # Marking Touches
        Vl.new()
          |> Vl.mark(:point, color: "purple", size: 500)
          |> Vl.transform(filter: "datum.first_touch === true") # Only include events where `event` is true
          |> Vl.encode_field(:x, "timestamp", type: :temporal)
          |> Vl.encode_field(:y, "close", type: :quantitative, scale: [zero: false]),
      ])
  end

end
