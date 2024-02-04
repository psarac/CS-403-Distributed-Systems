
import random as rnd
from threading import Thread, Lock, Barrier
from ConSet import ConSet

n = 4 # number of nodes
node_mailboxes = [ConSet() for i in range(n)] # mailboxes
barrier = Barrier(n) # to be sure that nodes moving to the next round together
mtx = Lock() # for the print statements

def nodeWork(node_id, n):
    
    round_counter = 0
    
    while True: 
        
        round_counter += 1
    
        # generating random number
        proposed_num = rnd.randint(0, n**2)
        
        mtx.acquire()
        print(f"Node {node_id} proposes value {proposed_num} for round {round_counter}.")
        mtx.release()
        
        new_msg = (node_id, proposed_num)
    
        # sending the message to all nodes
        for mailbox in node_mailboxes:
            
            mailbox.insert(new_msg)
        
        leader_node = -1
        leading_num = -1
        max_repeating_num = -1 # to track whether the current leader is repeating 
        
        for i in range(n):
            
            mail = node_mailboxes[node_id].pop()
            
            num = mail[1]
            sender = mail[0]
            
            if num > leading_num: # new leader found
                
                leading_num = num
                leader_node = sender
            
            elif num == leading_num: # current leader is repeating
                
                leader_node = sender  
                max_repeating_num = num   
                
        # if the final leader is same with lastly reported repeating leader, 
        #  nodes should go to next round
        if leading_num == max_repeating_num:
            
            mtx.acquire()
            print(f"Node {node_id} could not decide on the leader and moves to round {round_counter+1}.")
            mtx.release()
            
        else: # if the leader is unique, they can stop
            
            mtx.acquire()
            print(f"Node {node_id} decided {leader_node} as the leader.")
            mtx.release()
            break 
            
        barrier.wait()
            
            
if __name__ == "__main__":          
            
    threads = []
     
    # creating and joining the threads
    for i in range(n):
          
        thread = Thread(target=nodeWork, args=(i, n))
        threads.append(thread)
        thread.start()
        
    for i in range(n):
        
        threads[i].join()
            
            
            
            
            
            
            
            
        
        
    