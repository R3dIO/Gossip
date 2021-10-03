
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type GossipMessageTypes =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | StartGossip of String
    | ReportMsgRecvd of String
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | Result of Double * Double
    | Time of int
    | TotalNodes of int
    | ActivateWorker
    | CallWorker
    | AddNeighbors

let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)
let timer = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")

let mutable actualNumOfNodes = nodes |> float
nodes <-
    match topology with
    | "2D" | "Imp2D" -> 
        ((actualNumOfNodes ** 0.5) |> ceil ) ** 2.0 |> int
    | "3D" | "Imp3D" ->
        ((actualNumOfNodes ** 0.333) |> ceil ) ** 3.0 |> int
    | _ -> nodes
let mutable  nodeArray = [||]

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

//-------------------------------------- Supervisor Actor --------------------------------------//
let Supervisor(mailbox: Actor<_>) =
    
    let mutable count = 0
    let mutable start = 0
    let mutable totalNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | ReportMsgRecvd _ -> 
            let ending = DateTime.Now.TimeOfDay.Milliseconds
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Result (sum, weight) ->
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Time strtTime -> start <- strtTime
        | TotalNodes n -> totalNodes <- n
        | _ -> ()

        return! loop()
    }            
    loop()


let supervisor = spawn system "Supervisor" Supervisor
let dictionary = new Dictionary<IActorRef, bool>()
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

        | Initailize aref ->
            neighbours <- aref

        | ActivateWorker ->
            if rumourCount < 11 then
                let rnd = Random().Next(0, neighbours.Length)
                if not dictionary.[neighbours.[rnd]] then
                    neighbours.[rnd] <! CallWorker
                mailbox.Self <! ActivateWorker

        | CallWorker ->
            
            if rumourCount = 0 then 
                mailbox.Self <! ActivateWorker
            if (rumourCount = 10) then 
                supervisor <! ReportMsgRecvd "Rumor"
                dictionary.[mailbox.Self] <- true
            rumourCount <- rumourCount + 1
            
        | InitializeVariables number ->
            sum <- number |> double

        | StartPushSum delta ->
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! ComputePushSum(sum, weight, delta)

        | ComputePushSum (s: float, w, delta) ->
            let newsum = sum + s
            let newweight = weight + w

            let cal = sum / weight - newsum / newweight |> abs

            if alreadyConverged then

                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(s, w, delta)
            
            else
                if cal > delta then
                    termRound <- 0
                else 
                    termRound <- termRound + 1

                if  termRound = 3 then
                    termRound <- 0
                    alreadyConverged <- true
                    supervisor <! Result(sum, weight)
            
                sum <- newsum / 2.0
                weight <- newweight / 2.0
                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(sum, weight, delta)
        | _ -> ()
        return! loop()
    }            
    loop()



let ActorWorker (mailbox: Actor<_>) =
    let neighbors = new List<IActorRef>()
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | AddNeighbors _ ->
            for i in [0..nodes-1] do
                    neighbors.Add nodeArray.[i]
            mailbox.Self <! ActivateWorker
        | ActivateWorker ->
            if neighbors.Count > 0 then
                let randomNumber = Random().Next(neighbors.Count)
                let randomActor = neighbors.[randomNumber]
                
                if (dictionary.[neighbors.[randomNumber]]) then  
                    (neighbors.Remove randomActor) |> ignore
                else 
                    randomActor <! CallWorker
                mailbox.Self <! ActivateWorker 
        | _ -> ()
        return! loop()
    }
    loop()


let GossipActor = spawn system "ActorWorker" ActorWorker
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
nodeArray <- Array.zeroCreate (nodes + 1)
for x in [0..nodes] do
    let key: string = "worker" + string(x) 
    let actorRef = spawn system (key) Worker
    nodeArray.[x] <- actorRef 
    dictionary.Add(nodeArray.[x], false)
    nodeArray.[x] <! InitializeVariables x

match topology with
| "line" ->

    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        if i = 0 then
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[i+1] |])
        elif i = nodes then
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[i-1] |])
        else 
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[(i - 1)] ; nodeArray.[(i + 1 ) ] |] ) 
        
        nodeArray.[i] <! Initailize(neighbourArray)  

| "full" ->
    
    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        for j in [0..nodes] do 
            if i <> j then
                neighbourArray <- (Array.append neighbourArray [|nodeArray.[j]|])
        nodeArray.[i]<! Initailize(neighbourArray)

| "2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int 

    for y in [ 0 .. (gridSize-1)] do
        for x in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if x + 1 < gridSize then
                neighbours <- (Array.append neighbours [| nodeArray.[ (x + 1) + y * gridSize] |])
            if  x - 1 >= 0 then 
                neighbours <- (Array.append neighbours [| nodeArray.[ (x - 1) + y * gridSize] |])
            if y - 1 >= 0 then
                neighbours <- (Array.append neighbours [| nodeArray.[ x + ((y - 1 ) * gridSize)] |])
            if  y + 1 < gridSize then
                neighbours <- (Array.append neighbours [| nodeArray.[ x + ((y + 1) * gridSize)] |])
            nodeArray.[y * gridSize + x] <! Initailize(neighbours)

| "Imp2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int

    for y in [ 0 .. (gridSize-1)] do
        for x in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if x + 1 < gridSize then
                neighbours <- (Array.append neighbours [| nodeArray.[ (x + 1) + y * gridSize] |])
            if  x - 1 >= 0 then 
                neighbours <- (Array.append neighbours [| nodeArray.[ (x - 1) + y * gridSize] |])
            if y - 1 >= 0 then
                neighbours <- (Array.append neighbours [| nodeArray.[ x + ((y - 1 ) * gridSize)] |])
            if  y + 1 < gridSize then
                neighbours <- (Array.append neighbours [| nodeArray.[ x + ((y + 1) * gridSize)] |])
            let rnd = Random().Next(0, nodes-1)
            neighbours <- (Array.append neighbours [| nodeArray.[rnd] |])
            nodeArray.[y * gridSize + x] <! Initailize(neighbours)

| "3D" ->
    let gridSize = nthroot (float 3) (float nodes) |> ceil |> int

    for z in [ 0 .. (gridSize - 1)] do
        for y in [ 0 .. (gridSize - 1)] do
            for x in [ 0 .. (gridSize - 1)] do
                let mutable neighbours: IActorRef [] = [||]
                if  x - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x - 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  x + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x + 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  y + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + ((y + 1) * gridSize) + z * (pown gridSize 2)] |])
                if  y - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + ((y - 1) * gridSize) + z * (pown gridSize 2)] |])
                if  z + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + (y * gridSize) + ((z + 1) * (pown gridSize 2))] |])
                if  z - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + (y * gridSize) + ((z - 1) * (pown gridSize 2))] |])
                nodeArray.[x + (y * gridSize) + (z  * (pown gridSize 2))] <! Initailize(neighbours)


| "Imp3D" ->
    let gridSize = nthroot (float 3) (float nodes) |> ceil |> int

    for z in [ 0 .. (gridSize - 1)] do
        for y in [ 0 .. (gridSize - 1)] do
            for x in [ 0 .. (gridSize - 1)] do
                let mutable neighbours: IActorRef [] = [||]
                if  x - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x - 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  x + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x + 1) + (y * gridSize) + z * (pown gridSize 2)] |])
                if  y + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + ((y + 1) * gridSize) + z * (pown gridSize 2)] |])
                if  y - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + ((y - 1) * gridSize) + z * (pown gridSize 2)] |])
                if  z + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + (y * gridSize) + ((z + 1) * (pown gridSize 2))] |])
                if  z - 1 >= 0 then
                    neighbours <- (Array.append neighbours [| nodeArray.[(x) + (y * gridSize) + ((z - 1) * (pown gridSize 2))] |])
                let rnd = Random().Next(0, nodes-1)
                neighbours <- (Array.append neighbours [| nodeArray.[rnd] |])
                nodeArray.[x + (y * gridSize) + (z  * (pown gridSize 2))] <! Initailize(neighbours)
| _ -> ()

timer.Start()
let leader = Random().Next(0, nodes)
//Choose a random worker to start the gossip

match protocol with
| "gossip" -> 
    match topology with
    | "line" | "2D" | "Imp2D" | "3D" | "Imp3D" ->
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Executing Gossip Protocol for fixed Geometery"
        printfn "------------- Start Gossip -------------"
        nodeArray.[leader] <! ActivateWorker
        GossipActor <! AddNeighbors
    | "full" ->
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Executing Gossip Protocol for full network"
        printfn "------------- Start Gossip -------------"
        nodeArray.[leader] <! CallWorker
    | _ ->
        printfn "Invlaid topology" 
| "push-sum" -> 
    supervisor <! TotalNodes(nodes)
    supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
    printfn "Starting Push Sum Protocol"
    nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)     
| _ ->
    printfn "Invlaid protocol" 

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

