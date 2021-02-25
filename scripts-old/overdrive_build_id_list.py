import os
import sys

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: %s [data storage directory]" % sys.argv[0])
        sys.exit()
    path = sys.argv[1]      
    bibliographic_path = os.path.join(path, "Overdrive", "bibliographic")
    output_path = os.path.join(path, "Overdrive", "seed_ids.list")
    output = open(output_path, "w")
    ids = []
    for dir in os.listdir(bibliographic_path):
        p = os.path.join(bibliographic_path, dir)
        if not os.path.isdir(p):
            continue
        counter = 0
        for filename in os.listdir(p):
            if filename.endswith('.json'):
                output.write(filename[:-5])
                output.write("\n")
                output.flush()
                counter += 1
                if not counter % 1000:
                    print(counter)
    
