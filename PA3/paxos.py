
import argparse
import random
import zmq
from multiprocessing import Process, Barrier

# Sending method that sends messages over ZeroMQ sockets
def send(msg, target):
    
    target.send_string(msg)

# Send message with a probability of failure
def sendFailure(msg, proposer, target, prob):
    
    rand = random.random()
    
    if rand > prob: # No failure
        send(msg, target)
    else: # Node failed
       crash_msg = f"CRASH {proposer}"
       send(crash_msg, target) 

# Broadcasts a message with a probability of failure 
def broadcastFailure(msg, proposer, N, prob, socket_list):
    
    for i in range(N):
        
        sendFailure(msg, proposer, socket_list[i], prob)

# Paxos Node Process
def PaxosNode(ID, prob, N, val, numRounds, barrier):
    
    # Initialization of ZeroMQ sockets
    context = zmq.Context()
    base_port = 5550
    
    # Push Sockets
    socket_list = []
    
    for n in range(N):
        
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect("tcp://127.0.0.1:" + str(base_port+n))
        socket_list.append(push_socket)
    
    # Pull Socket 
    pull_socket = context.socket(zmq.PULL)
    pull_socket.bind("tcp://127.0.0.1:" + str(base_port+ID))
    
    isProposer = False # Whether the process is leader in the current round 
    
    maxVotedRound = -1
    maxVotedValue = None 
    
    proposeVal = None # Proposed value from the last time this node was proposer
    decision = None # If proposer can gather enough nodes, it becomes proposeVal 
    
    for r in range(numRounds):
        
        # Decide whether the node is leader this round 
        if r % N == ID: 
            isProposer = True
        else:
            isProposer = False
            
        if isProposer: # Proposer
        
            print(f"ROUND {r} STARTED WITH INITIAL VALUE {val}")
            
            # Starting the round
            broadcastFailure(f"START", ID, N, prob, socket_list)
            
            joined = 0
            currentMaxRound = -1 # To track the message with maxVotedRound
            currentMaxVal = -1 # To track the value of such message 
            
            for i in range(N):
                
                # Receive messages from all nodes, including itself
                msg = pull_socket.recv_string()
                print(f"LEADER OF {r} RECEIVED IN JOIN PHASE: {msg}", flush=True)
                
                split_msg = msg.split()
                
                # A node has successfully joined 
                if split_msg[0] == "JOIN":
                    
                    joined += 1 
                    rnd = int(split_msg[1])
                    
                    if rnd > currentMaxRound: 
                        currentMaxRound = rnd
                        currentMaxVal = int(split_msg[2])
                        
                 
                # Proposer successfully joined 
                elif split_msg[0] == "START":
                    
                    joined += 1 
                    
                    if maxVotedRound > currentMaxRound: 
                        currentMaxRound = maxVotedRound
                        currentMaxVal = maxVotedValue
                        
              
            # If join quorum has formed 
            if joined > N / 2:
                
                # Propose the value from the node that has voted most recently
                #  Otherwise, propose the default value of the node 
                if currentMaxRound == -1:
                    proposeVal = val
                else:
                    proposeVal = currentMaxVal
                    
                propose_msg = f"PROPOSE {proposeVal}"
                broadcastFailure(propose_msg, ID, N, prob, socket_list)
                
                voted = 0 # To count the voters 
                
                for i in range(N):
                    
                    # Receive messages from all nodes, including itself
                    msg = pull_socket.recv_string()
                    print(f"LEADER OF {r} RECEIVED IN VOTE PHASE: {msg}", flush=True)
                    
                    split_msg = msg.split()
                    
                    if split_msg[0] == "VOTE":
                        voted += 1 # A node has successfully voted 
                    elif split_msg[0] == "PROPOSE":
                        # Proposer has successfully voted
                        voted += 1
                        maxVotedRound = r
                        maxVotedValue = proposeVal
                     
                # If vote quorum formed, make a decision 
                if voted > N / 2: 
                    decision = proposeVal 
                    print(f"LEADER OF {r} DECIDED ON VALUE {decision}", flush=True)
                
                barrier.wait()
                    
                
            else: # Roundchange as join quorum is not formed 
            
                print(f"LEADER OF ROUND {r} CHANGED ROUND", flush=True)
            
                change_msg = "ROUNDCHANGE"
            
                for i in range(N):
                    if i != ID:
                        send(change_msg, socket_list[i])
                
                barrier.wait()
                        
        else: # Acceptor
            
            # Blocks for a message from leader
            msg = pull_socket.recv_string()
            print(f"ACCEPTOR {ID} RECEIVED IN JOIN PHASE: {msg}", flush=True)
            
            split_msg = msg.split()
            
            # Node received Start so it can join 
            if split_msg[0] == "START":
                response_msg = f"JOIN {maxVotedRound} {maxVotedValue}"
                sendFailure(response_msg, r % N, socket_list[r%N], prob)
                
            else: # Proposer crashed, node response to crash 
                crash_response = f"CRASH {r%N}"
                send(crash_response, socket_list[r%N])
                
            
            # Blocks for the proposal
            propose_msg = pull_socket.recv_string()
            print(f"ACCEPTOR {ID} RECEIVED IN VOTE PHASE: {propose_msg}", flush=True)
            
            split_propose = propose_msg.split()
            
            # Node receive dthe proposal, so it votes 
            if split_propose[0] == "PROPOSE":
                sendFailure("VOTE", r%N, socket_list[r%N], prob)
                maxVotedRound = r
                maxVotedValue = int(split_propose[1])
                
                barrier.wait()
               
            # Proposal did not reach to node due to crash, node responds with crash 
            elif split_propose[0] == "CRASH":
                send(f"CRASH {r%N}", socket_list[r%N])
                
                barrier.wait()
                
            else: # Roundchange as join quroum has not formed 
            
                barrier.wait()
                continue
           
            
if __name__ == "__main__":
    
    argParser = argparse.ArgumentParser()
    
    # Gathering all arguments needed for Paxos 
    argParser.add_argument("numProc", type=int, help="Number of total nodes in Paxos")
    argParser.add_argument("prob", type=float, help="Probability of failure for a node")
    argParser.add_argument("numRounds", type=int, help="Number of Paxos rounds")
    
    args = argParser.parse_args()
    
    barrier = None

    # Checking argument validity
    
    if args.numProc < 0:
        print("Number of nodes cannot be negative!")
    
    elif args.prob < 0 or args.prob > 1:
        print("Failure probability must be between 0 and 1!")
    
    elif args.numRounds < 0:
        print("Number of rounds cannot be negative!")
        
    else:
        
        # If arguments are valid, real process can start
        
        print(f"NUM_NODES: {args.numProc}, CRASH PROB: {args.prob}, NUM_ROUNDS: {args.numRounds}")
        
        barrier = Barrier(args.numProc)
        
        PaxosNodes = []
        
        # Creating the Paxos nodes 
        for i in range(args.numProc):
             
            proc = Process(target=PaxosNode, args=(i, args.prob, args.numProc, i % 2, args.numRounds, barrier,))
            PaxosNodes.append(proc)
            proc.start()
            
        for i in range(args.numProc):
            
            PaxosNodes[i].join()

    
    