
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type GossipMessageTypes =
    | Time of int
    | TotalNodes of int
    | InitailizeNeighbours of IActorRef []
    | InitializeVariables of int
    | ShareGossip of bool
    | CallWorker
    | ConvergeGossip
    | StartPushSum of Double
    | CalculatePushSum of Double * Double * Double * bool
    | ConvergePushSum of Double * Double
    | ActivateGossipWorker of List<IActorRef> * int
    | ActivatePushSumWorker of List<IActorRef> * int
    | GetNeighborsToIgnore of int []

let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let algorithm = string (fsi.CommandLineArgs.GetValue 3)
let numNodesToIgnore = string (fsi.CommandLineArgs.GetValue 4) |>int

let mutable shutdownNeighborsList : int [] = [||]
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")

let mutable normalizedNumOfNodes = nodes |> float
nodes <-
    match topology with
    | "2D" | "Imp2D" -> 
        ((normalizedNumOfNodes ** 0.5) |> ceil ) ** 2.0 |> int
    | "3D" | "Imp3D" ->
        ((normalizedNumOfNodes ** 0.333) |> ceil ) ** 3.0 |> int
    | _ -> nodes
let mutable globalNodeArray = [||]
//-------------------------------------- Initialization --------------------------------------//

//-------------------------------------- Utils --------------------------------------//
let nthroot n A =
    let rec f x =
        let m = n - 1.
        let x' = (m * x + A/x**m) / n
        match abs(x' - x) with
        | t when t < abs(x * 1e-9) -> x'
        | _ -> f x'
    f (A / double n)
//-------------------------------------- Utils --------------------------------------//

//-------------------------------------- Master Actor --------------------------------------//
let Master(mailbox: Actor<_>) =
    
    let mutable nodeCount = 0
    let mutable totalNumNodes = 0
    let mutable start = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | Time strtTime -> start <- strtTime
        | TotalNodes n -> totalNumNodes <- n
        | ConvergeGossip -> 
            nodeCount <- nodeCount + 1
            if nodeCount = totalNumNodes then
                stopWatch.Stop()
                printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                printfn "------------- End Gossip -------------"
                Environment.Exit(0)
        | ConvergePushSum (sum, weight) ->
            nodeCount <- nodeCount + 1
            if nodeCount = totalNumNodes then
                stopWatch.Stop()
                printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                printfn "Delta value of convergence: %f sum = %f & weight = %f" (sum/weight) sum weight
                printfn "------------- End Push-Sum -------------"
                Environment.Exit(0)
        | _ -> ()

        return! loop()
    }            
    loop()

let master = spawn system "Master" Master
let saturatedNodesDict = new Dictionary<IActorRef, bool>()
let indexDictionary = new Dictionary<IActorRef, int>()

//-------------------------------------- Supervisor Actor --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
    // Activate Gossip worker first initialize neighbour and then select a random node for Gossip
let GossipActor (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | ActivateGossipWorker (neighborsList: List<IActorRef>, neighboursToActivate: int) ->
            if neighborsList.Count > 0 then
                let randomNumber = Random().Next(neighborsList.Count)
                let randomActor = neighborsList.[randomNumber]
                
                // Check if node is not converged then send the message
                if (saturatedNodesDict.[randomActor]) then  
                    (neighborsList.Remove randomActor) |> ignore
                else 
                    randomActor <! CallWorker

                // if(neighboursToActivate > 0) then
                mailbox.Self <! ActivateGossipWorker(neighborsList, (neighboursToActivate - 1))
        | _ -> ()
        return! loop()
    }
    loop()

let PushSumActor (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | ActivatePushSumWorker (neighborsList: List<IActorRef>, neighboursToActivate: int) ->
            if neighborsList.Count > 0 then
                let randomNumber = Random().Next(neighborsList.Count)
                let randomActor = neighborsList.[randomNumber]
                if (saturatedNodesDict.[randomActor]) then  
                    (neighborsList.Remove randomActor) |>ignore
                else 
                    randomActor <! StartPushSum(10.0 ** -10.0)
                mailbox.Self <! ActivatePushSumWorker (neighborsList, neighboursToActivate - 1)

        | _ -> ()
        return! loop()
    }
    loop()

let GossipWorker (mailbox: Actor<_>) =
    let mutable rumoursHeard = 0
    let mutable neighboursList: IActorRef [] = [||]
    let mutable neighborsToIgoreArray:int [] = [||]
    let mutable faultynode = false
 
    let rec loop()= actor{
        let! message = mailbox.Receive();
        try
            match message with
            | InitailizeNeighbours aref ->
                neighboursList <- aref

            | GetNeighborsToIgnore shutdownNeighborsList ->
                neighborsToIgoreArray <-shutdownNeighborsList
                if Array.contains indexDictionary.[mailbox.Self] neighborsToIgoreArray  then
                    faultynode <- true

            | ShareGossip isActive ->
                if (not faultynode || isActive) then
                    if rumoursHeard < 11 then
                        let rnd = Random().Next(0, neighboursList.Length)
                        if not saturatedNodesDict.[neighboursList.[rnd]] then
                            neighboursList.[rnd] <! CallWorker
                        mailbox.Self <! ShareGossip false

            | CallWorker ->
                if rumoursHeard = 0 then 
                    mailbox.Self <! ShareGossip true
                if (not saturatedNodesDict.[mailbox.Self]) && (rumoursHeard = 11) then 
                    master <! ConvergeGossip
                    saturatedNodesDict.[mailbox.Self] <- true

                rumoursHeard <- rumoursHeard + 1
            | _ -> ()
        with
            | :? System.IndexOutOfRangeException -> printfn "Tried to access outside array!" |> ignore
        return! loop()
    }            
    loop()

let PushSumWorker (mailbox: Actor<_>) =
    let mutable sum = 0 |> double
    let mutable weight = 1.0
    let mutable termRound = 1
    let mutable neighbours: IActorRef [] = [||]
    let mutable isConverged = false
    let mutable neighborsToIgoreArray:int [] = [||]
    let mutable faultynode = false 
    
    let rec loop()= actor{
        let! message = mailbox.Receive();
        
        match message with 

        | InitializeVariables number ->
            sum <- number |> double

        | GetNeighborsToIgnore shutdownNeighborsList ->
            neighborsToIgoreArray <- shutdownNeighborsList
            if Array.contains indexDictionary.[mailbox.Self] neighborsToIgoreArray  then
                faultynode <- true

        | InitailizeNeighbours aref ->
            neighbours <- aref

        | StartPushSum delta ->
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! CalculatePushSum(sum, weight, delta, true)

        | CalculatePushSum (s: float, w, delta, isActive) ->
            if (not faultynode || isActive) then
                let index = Random().Next(0, neighbours.Length)
                if isConverged then
                   neighbours.[index] <! CalculatePushSum(s, w, delta, false)
                else
                    let updatedSum = sum + s
                    let updatedWeight = weight + w
                    let change = (sum / weight) - (updatedSum / updatedWeight) |> abs

                    if change < delta then
                        termRound <- termRound + 1
                    else 
                        termRound <- 0

                    if  termRound = 3 then
                        isConverged <- true
                        master <! ConvergePushSum(sum, weight)

                    sum <- updatedWeight / 2.0
                    weight <- updatedWeight / 2.0
                    neighbours.[index] <! CalculatePushSum(sum, weight, delta, true)
        | _ -> ()
        return! loop()
    }            
    loop()

//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
globalNodeArray <- Array.zeroCreate (nodes + 1)
for x in [0..nodes] do
    match algorithm with
    | "gossip" -> 
        let key: string = "GossipWorker" + string(x)
        globalNodeArray.[x] <- spawn system (key) GossipWorker
    | "push-sum" ->
        let key: string = "PushSumWorker" + string(x)
        globalNodeArray.[x] <- spawn system (key) PushSumWorker
    | _ -> Environment.Exit(0)
    saturatedNodesDict.Add(globalNodeArray.[x], false)
    indexDictionary.Add(globalNodeArray.[x], x)
    globalNodeArray.[x] <! InitializeVariables x

for i = 1 to numNodesToIgnore do   
    let mutable rndNodeToShutdown = Random().Next(0, nodes)
    // printfn  "shutting down %i"  rndNodeToShutdown
    shutdownNeighborsList <- (Array.append shutdownNeighborsList [|rndNodeToShutdown|])
[0..nodes] |> List.iter (fun i -> globalNodeArray.[i] <! GetNeighborsToIgnore(shutdownNeighborsList))
 
match topology with
| "line" ->
    for i in [ 0 .. nodes ] do
        let mutable neighbourArr = [||]
        if i = 0 then
            neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[i+1] |])
        elif i = nodes then
            neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[i-1] |])
        else 
            neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(i - 1)] ; globalNodeArray.[(i + 1 ) ] |] ) 
        
        globalNodeArray.[i] <! InitailizeNeighbours(neighbourArr)  

| "full" ->
    for i in [ 0 .. nodes ] do
        let mutable neighbourArr = [||]
        for j in [0..nodes] do 
            if i <> j then
                neighbourArr <- (Array.append neighbourArr [|globalNodeArray.[j]|])
        globalNodeArray.[i]<! InitailizeNeighbours(neighbourArr)

| "2D" | "Imp2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int

    for y in [ 0 .. (gridSize-1)] do
        for x in [ 0 .. (gridSize-1) ] do
            let mutable neighbourArr: IActorRef [] = [||]
            if x + 1 < gridSize then
                neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[ (x + 1) + y * gridSize] |])
            if  x - 1 >= 0 then 
                neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[ (x - 1) + y * gridSize] |])
            if y - 1 >= 0 then
                neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[ x + ((y - 1 ) * gridSize)] |])
            if  y + 1 < gridSize then
                neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[ x + ((y + 1) * gridSize)] |])
            if topology = "Imp2D" then
                let rnd = Random().Next(0, nodes-1)
                neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[rnd] |])
            globalNodeArray.[y * gridSize + x] <! InitailizeNeighbours(neighbourArr)
   
| "3D" | "Imp3D" ->
    let gridSize = nthroot (float 3) (float nodes) |> ceil |> int

    for z in [ 0 .. (gridSize - 1)] do
        for y in [ 0 .. (gridSize - 1)] do
            for x in [ 0 .. (gridSize - 1)] do
                let mutable neighbourArr: IActorRef [] = [||]
                if  x - 1 >= 0 then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x - 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  x + 1 < gridSize then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x + 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  y + 1 < gridSize then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x) + ((y + 1) * gridSize) + z * (pown gridSize 2)] |])
                if  y - 1 >= 0 then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x) + ((y - 1) * gridSize) + z * (pown gridSize 2)] |])
                if  z + 1 < gridSize then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x) + (y * gridSize) + ((z + 1) * (pown gridSize 2))] |])
                if  z - 1 >= 0 then
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[(x) + (y * gridSize) + ((z - 1) * (pown gridSize 2))] |])
                if topology = "Imp3D" then
                    let rnd = Random().Next(0, nodes-1)
                    neighbourArr <- (Array.append neighbourArr [| globalNodeArray.[rnd] |])
                globalNodeArray.[x + (y * gridSize) + (z  * (pown gridSize 2))] <! InitailizeNeighbours(neighbourArr)

| _ -> ()

stopWatch.Start()
// Select a random worker to begin the gossip
let leader = Random().Next(0, nodes)
master <! TotalNodes(nodes)
master <! Time(DateTime.Now.TimeOfDay.Milliseconds)

match algorithm with
| "gossip" -> 
    printfn "------------- Start Gossip -------------"
    match topology with
    | "line" | "2D" | "Imp2D" | "3D" | "Imp3D" ->
        let GossipActorWorker = spawn system "ActorWorker" GossipActor
        let neighbors = new List<IActorRef>()
        printfn "Executing Gossip Protocol for fixed Geometery"
        globalNodeArray.[leader] <! ShareGossip
        for i in [0..nodes-1] do
            neighbors.Add globalNodeArray.[i]
        GossipActorWorker <! ActivateGossipWorker(neighbors, int(nodes))
    | "full" ->
        printfn "Executing Gossip Protocol for full network"
        globalNodeArray.[leader] <! CallWorker
    | _ ->
        printfn "Invalid topology" 
| "push-sum" ->
    let PushSumActorWorker = spawn system "PushSumActorWorker" PushSumActor
    let neighbors = new List<IActorRef>()
    for i in [0..nodes-1] do
        neighbors.Add globalNodeArray.[i]
    printfn "Starting Push Sum Protocol"
    printfn "------------- Start Push-Sum -------------"
    PushSumActorWorker <! ActivatePushSumWorker(neighbors, int(nodes))
    globalNodeArray.[leader] <! StartPushSum(10.0 ** -10.0)     
| _ ->
    printfn "Invalid protocol" 

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

