
from MapReduce import MapReduce

class FindCitations(MapReduce):
    
    def Map(self, packet):
        
        partial_result = {}
        
        loop_len = len(packet)//2 # every two node forms an edge
        
        for i in range(loop_len):
            
            cited = str(packet[2*i+1]) # node with incoming edge
            
            if cited not in partial_result.keys():
                partial_result[cited] = 1
            else:
                partial_result[cited] += 1
        
        return partial_result
    
    def Reduce(self, results):
        
        final_result = {}
        
        for result in results:
            
            for key in result.keys():
                
                if key not in final_result.keys():
                    final_result[key] = result[key]
                else:
                    final_result[key] += result[key]
        
        return final_result
                    