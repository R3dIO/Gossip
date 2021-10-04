
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
    | StartGossip of String
    | ShareGossip
    | ConvergeGossip
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | ConvergePushSum of Double * Double
    | CallWorker
    | AddNeighbors
    | ActivateGossipWorker of List<IActorRef>

let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)
let timer = Diagnostics.Stopwatch()
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
    
    let mutable count = 0
    let mutable start = 0
    let mutable totalNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | Time strtTime -> start <- strtTime
        | TotalNodes n -> totalNodes <- n
        | ConvergeGossip -> 
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                printfn "------------- End Gossip -------------"
                Environment.Exit(0)
        | ConvergePushSum (sum, weight) ->
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                printfn "Delta value of convergence: %f sum = %f & weight = %f" (sum/weight) sum weight
                printfn "------------- End Push-Sum -------------"
                Environment.Exit(0)
        | _ -> ()

        return! loop()
    }            
    loop()

let master = spawn system "Master" Master
let saturatedNodesDict = new Dictionary<IActorRef, bool>()
//-------------------------------------- Supervisor Actor --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
let Worker(mailbox: Actor<_>) =
    let mutable rumourCount = 0
    let mutable neighbours: IActorRef [] = [||]
    let mutable sum = 0 |>double
    let mutable weight = 1.0
    let mutable termRound = 1
    let mutable alreadyConverged = false
    
    
    let rec loop()= actor{
        let! message = mailbox.Receive();
        
        match message with 

        | InitializeVariables number ->
            sum <- number |> double

        | InitailizeNeighbours aref ->
            neighbours <- aref

        | ShareGossip ->
            if rumourCount < 11 then
                let rnd = Random().Next(0, neighbours.Length)
                if not saturatedNodesDict.[neighbours.[rnd]] then
                    neighbours.[rnd] <! CallWorker
                mailbox.Self <! ShareGossip

        | CallWorker ->
            if rumourCount = 0 then 
                mailbox.Self <! ShareGossip
            if (not saturatedNodesDict.[mailbox.Self]) && (rumourCount = 15) then 
                master <! ConvergeGossip
                saturatedNodesDict.[mailbox.Self] <- true
            rumourCount <- rumourCount + 1

        | StartPushSum delta ->
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! ComputePushSum(sum, weight, delta)

        | ComputePushSum (s: float, w, delta) ->
            let newsum = sum + s
            let newweight = weight + w

            let diff = sum / weight - newsum / newweight |> abs

            if alreadyConverged then

                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(s, w, delta)
            
            else
                if diff > delta then
                    termRound <- 0
                else 
                    termRound <- termRound + 1

                if  termRound = 3 then
                    termRound <- 0
                    alreadyConverged <- true
                    master <! ConvergePushSum(sum, weight)
            
                sum <- newsum / 2.0
                weight <- newweight / 2.0
                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(sum, weight, delta)
        | _ -> ()
        return! loop()
    }            
    loop()


// Activate Gossip worker first initialize neighbour and then select a random node for Gossip
let GossipActor (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | ActivateGossipWorker neighbors ->
            if neighbors.Count > 0 then
                let randomNumber = Random().Next(neighbors.Count)
                let randomActor = neighbors.[randomNumber]
                
                // Check if node is not converged then send the message
                if (saturatedNodesDict.[neighbors.[randomNumber]]) then  
                    (neighbors.Remove randomActor) |> ignore
                else 
                    randomActor <! CallWorker
                mailbox.Self <! ActivateGossipWorker neighbors
        | _ -> ()
        return! loop()
    }
    loop()
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
globalNodeArray <- Array.zeroCreate (nodes + 1)
for x in [0..nodes] do
    let key: string = "worker" + string(x) 
    let actorRef = spawn system (key) Worker
    globalNodeArray.[x] <- actorRef 
    saturatedNodesDict.Add(globalNodeArray.[x], false)
    globalNodeArray.[x] <! InitializeVariables x

match topology with
| "line" ->

    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        let mutable localArray = [||]
        if i = 0 then
            neighbourArray <- (Array.append neighbourArray [| globalNodeArray.[i+1] |])
        elif i = nodes then
            neighbourArray <- (Array.append neighbourArray [| globalNodeArray.[i-1] |])
        else 
            neighbourArray <- (Array.append neighbourArray [| globalNodeArray.[(i - 1)] ; globalNodeArray.[(i + 1 ) ] |] ) 
        
        globalNodeArray.[i] <! InitailizeNeighbours(neighbourArray)  

| "full" ->
    
    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        for j in [0..nodes] do 
            if i <> j then
                neighbourArray <- (Array.append neighbourArray [|globalNodeArray.[j]|])
        globalNodeArray.[i]<! InitailizeNeighbours(neighbourArray)

| "2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int 

    for y in [ 0 .. (gridSize-1)] do
        for x in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if x + 1 < gridSize then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ (x + 1) + y * gridSize] |])
            if  x - 1 >= 0 then 
                neighbours <- (Array.append neighbours [| globalNodeArray.[ (x - 1) + y * gridSize] |])
            if y - 1 >= 0 then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ x + ((y - 1 ) * gridSize)] |])
            if  y + 1 < gridSize then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ x + ((y + 1) * gridSize)] |])
            globalNodeArray.[y * gridSize + x] <! InitailizeNeighbours(neighbours)

| "Imp2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int

    for y in [ 0 .. (gridSize-1)] do
        for x in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if x + 1 < gridSize then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ (x + 1) + y * gridSize] |])
            if  x - 1 >= 0 then 
                neighbours <- (Array.append neighbours [| globalNodeArray.[ (x - 1) + y * gridSize] |])
            if y - 1 >= 0 then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ x + ((y - 1 ) * gridSize)] |])
            if  y + 1 < gridSize then
                neighbours <- (Array.append neighbours [| globalNodeArray.[ x + ((y + 1) * gridSize)] |])
            let rnd = Random().Next(0, nodes-1)
            neighbours <- (Array.append neighbours [| globalNodeArray.[rnd] |])
            globalNodeArray.[y * gridSize + x] <! InitailizeNeighbours(neighbours)

| "3D" ->
    let gridSize = nthroot (float 3) (float nodes) |> ceil |> int

    for z in [ 0 .. (gridSize - 1)] do
        for y in [ 0 .. (gridSize - 1)] do
            for x in [ 0 .. (gridSize - 1)] do
                let mutable neighbours: IActorRef [] = [||]
                if  x - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x - 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  x + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x + 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  y + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + ((y + 1) * gridSize) + z * (pown gridSize 2)] |])
                if  y - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + ((y - 1) * gridSize) + z * (pown gridSize 2)] |])
                if  z + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + (y * gridSize) + ((z + 1) * (pown gridSize 2))] |])
                if  z - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + (y * gridSize) + ((z - 1) * (pown gridSize 2))] |])
                globalNodeArray.[x + (y * gridSize) + (z  * (pown gridSize 2))] <! InitailizeNeighbours(neighbours)


| "Imp3D" ->
    let gridSize = nthroot (float 3) (float nodes) |> ceil |> int

    for z in [ 0 .. (gridSize - 1)] do
        for y in [ 0 .. (gridSize - 1)] do
            for x in [ 0 .. (gridSize - 1)] do
                let mutable neighbours: IActorRef [] = [||]
                if  x - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x - 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  x + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x + 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  y + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + ((y + 1) * gridSize) + z * (pown gridSize 2)] |])
                if  y - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + ((y - 1) * gridSize) + z * (pown gridSize 2)] |])
                if  z + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + (y * gridSize) + ((z + 1) * (pown gridSize 2))] |])
                if  z - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| globalNodeArray.[(x) + (y * gridSize) + ((z - 1) * (pown gridSize 2))] |])
                let rnd = Random().Next(0, nodes-1)
                neighbours <- (Array.append neighbours [| globalNodeArray.[rnd] |])
                globalNodeArray.[x + (y * gridSize) + (z  * (pown gridSize 2))] <! InitailizeNeighbours(neighbours)
| _ -> ()

timer.Start()
// Select a random worker to begin the gossip
let leader = Random().Next(0, nodes)
master <! TotalNodes(nodes)
master <! Time(DateTime.Now.TimeOfDay.Milliseconds)

match protocol with
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
        GossipActorWorker <! ActivateGossipWorker neighbors
    | "full" ->
        printfn "Executing Gossip Protocol for full network"
        globalNodeArray.[leader] <! CallWorker
    | _ ->
        printfn "Invalid topology" 
| "push-sum" ->
    printfn "Starting Push Sum Protocol"
    printfn "------------- Start Push-Sum -------------"
    globalNodeArray.[leader] <! StartPushSum(10.0 ** -10.0)     
| _ ->
    printfn "Invalid protocol" 

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

