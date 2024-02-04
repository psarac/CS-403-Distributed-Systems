
from abc import ABC, abstractmethod
from multiprocessing import Process, Array, Barrier
import zmq

class MapReduce(ABC):
    
    def __init__(self, num_worker):
        self.num_worker = num_worker
        self.barrier = Barrier(self.num_worker+1)
        #self.barrier2 = Barrier(self.num_worker+1)
        self.barrier3 = Barrier(self.num_worker+1)
        
    @abstractmethod    
    def Map(self, map_input):
        pass
    
    @abstractmethod
    def Reduce(self, reduce_input):
        pass
    
    def _Producer(self, len_of_input, producer_input):
        
        # Deciding on the number of packets per worker
        num_of_lines = len_of_input//2
        lines_per_worker = num_of_lines//self.num_worker 
        
        remaining = num_of_lines - (self.num_worker * lines_per_worker)
        
        elt = 0 # index for elements of data array 
        
        context = zmq.Context()
        base_port = 5558
        
        self.barrier.wait()
        
        for i in range(self.num_worker):
            
            push_socket = context.socket(zmq.PUSH)
            push_socket.connect("tcp://127.0.0.1:" + str(base_port+i))
            
            # Preparing the packet
            packet = [producer_input[elt + j] for j in range(2*lines_per_worker)]
            elt += 2*lines_per_worker
            
            if remaining != 0:
                
                packet.append(producer_input[elt])
                packet.append(producer_input[elt+1])
                
                elt += 2 
                remaining -= 1
                
            # Pushing the packet to pipeline 
            work_message = {'packet': packet}
            push_socket.send_json(work_message)
            
            push_socket.close()
            
        
        
    def _Consumer(self, port):
        
        
        # Creating the pull socket 
        context = zmq.Context()
        pull_socket = context.socket(zmq.PULL)
        pull_socket.bind("tcp://127.0.0.1:" + str(port))
        
        self.barrier.wait()
        
        # Receving the packet of the worker
        received = pull_socket.recv_json()
        packet = received['packet']
        
        pull_socket.close()
    
        
        # Processing the packet
        partial_result = self.Map(packet)
        
        # Creating push socket
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect("tcp://127.0.0.1:" + str(port+10))
        
        result_message = {'partial_result': partial_result}
        push_socket.send_json(result_message)
        
        self.barrier3.wait()
        
        
    def _ResultCollector(self):
        
        # Creating pull socket
        context = zmq.Context()
        
        base_port = 5568
        results = []
        
        # Receiving the partial results from the workers 
        for i in range(self.num_worker):
            
            pull_socket = context.socket(zmq.PULL)
            pull_socket.bind("tcp://127.0.0.1:" + str(base_port + i))
        
            received = pull_socket.recv_json()
            partial_result = received['partial_result']
            results.append(partial_result)
            
            pull_socket.close()
            
        self.barrier3.wait()
        
        
        # Reducing partial results into the final result 
        final_result = self.Reduce(results)
        
        # Writing the results into results.txt
        file = open("results.txt", 'w')
        file.write(str(final_result))
        file.close()
        
        
        
    def start(self, filename):
        
        data_list = []
        
        # Reading the file
        citation_file = open(filename, 'r')
        lines = citation_file.readlines()
        citation_file.close()
        
        for line in lines:
            
            line = line.strip() 
            line = line.split('\t')
            
            #data_list.append(line)
            data_list.append(int(line[0])) # from
            data_list.append(int(line[1])) # to 
            
        len_of_list = len(data_list)
        data_array = Array('i', data_list)

        # Producer process
        Producer = Process(target=self._Producer, args=(len_of_list, data_array,))
        Producer.start()
        
        # Consumer processes
        consumer_processes = []
        base_port = 5558
        
        for i in range(self.num_worker):
            
            consumer = Process(target=self._Consumer, args=(base_port+i,))
            consumer_processes.append(consumer)
            consumer.start()
            
        # Result Collector process
        ResultCollector = Process(target=self._ResultCollector)
        ResultCollector.start()
            
        # Join every process
        Producer.join()
        
        for i in range(self.num_worker):
            
            consumer_processes[i].join()
            
        ResultCollector.join()
            
            
        
