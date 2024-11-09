defmodule Benchmarks.Labeling do

  def run() do
    Benchee.run(
      %{
        "Labeling: Triple Barrier Method" => fn -> Labeling.compute_tripple_barriers() end,
        },
      time: 10
    )


  end


end
