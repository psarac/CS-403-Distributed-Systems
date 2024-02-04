
from MapReduce import MapReduce

class FindCyclicReferences(MapReduce):
    
    def Map(self, packet):
        
        partial_result = {}
        
        loop_len = len(packet)//2 # every two node forms an edge
        
        for i in range(loop_len):
            
            from_node = packet[2*i]
            to_node = packet[2*i+1]
            
            key_format = "({minimum}, {maximum})"
            result_key = key_format.format(maximum=max(from_node, to_node), minimum=min(from_node, to_node))
            
            if result_key not in partial_result.keys():
                partial_result[result_key] = 0
            else:
                partial_result[result_key] = 1
                
        return partial_result
    
    def Reduce(self, results):
        
        final_result = {}
        
        for result in results:
            
            for key in result.keys():
                
                if key not in final_result.keys():
                    final_result[key] = result[key]
                else:
                    final_result[key] = 1 
        
        final_result = {key:value for key, value in final_result.items() if value != 0}
        
        return final_result
                    