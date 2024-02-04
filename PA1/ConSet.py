# Fill the class given below for the first part of the assignment
# You can add new functions if necessary
# but don't change the signatures of the ones included

from threading import Semaphore

class ConSet:
    
    def __init__(self):
        
        self.set_state = {} # dictionary representing the set state
        self.mtx = Semaphore(1) # mutex for manipulating the set
        self.sem = Semaphore(0) # semaphore related with number of elements in the set

    def insert(self, newItem):
        
        self.mtx.acquire()
        self.set_state[newItem] = True
        self.mtx.release()
        self.sem.release()
        
    def pop(self):
        
        self.sem.acquire()
        self.mtx.acquire() 
        
        for key in self.set_state.keys():
            
            if self.set_state[key]:
                
                self.set_state[key] = False
                self.mtx.release()
                return key 
        else:
            self.mtx.release() # just in case 

    def printSet(self):
        
        self.mtx.acquire()
        print(self.set_state)
        self.mtx.release()
        
        
        
            
            
        
