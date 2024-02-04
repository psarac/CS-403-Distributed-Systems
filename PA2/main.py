
import argparse
import os
from FindCitations import FindCitations
from FindCyclicReferences import FindCyclicReferences

def main_flow(args):

    # Checking the argument validity
    
    #  Number of workers
    if args.num_worker > 10:
        print("Maximum number of workers can be 10!")
        return 
    elif args.num_worker < 1:
        print("Must have at least 1 worker!")
        return
    #  File 
    elif not os.path.isfile(args.filename):
        print("No such file in the current directory!")
        return
    
    # Main flow
    processor = FindCitations(args.num_worker) if args.function == "COUNT" else FindCyclicReferences(args.num_worker)
    processor.start(args.filename)

if __name__ == "__main__":
    
    argParser = argparse.ArgumentParser()
    
    # Arguments needed for the flow
    argParser.add_argument("function", choices=["COUNT", "CYCLE"], help="Choose a function (COUNT or CYCLE)")
    argParser.add_argument("num_worker", type=int, help="Number of workers (max 10)")
    argParser.add_argument("filename", type=str, help="File to process")
    
    args = argParser.parse_args()
    
    main_flow(args)
    
    

    

    
    
    
    
    
    
    


