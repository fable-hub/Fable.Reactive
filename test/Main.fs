module Tests.Main

open Expecto

[<Tests>]
let allTests =
    testList
        "all-tests"
        [ AsyncSeq.tests
          Bind.tests
          Catch.tests
          Concat.tests
          Create.tests
          Filter.tests
          Map.tests
          Merge.tests
          Observer.tests
          Scan.tests
          Timeshift.tests
          ActorInterop.tests]

[<EntryPoint>]
let main argv =
    printfn "Running tests!"
    runTestsWithCLIArgs [] argv allTests
