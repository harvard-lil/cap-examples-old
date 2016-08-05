import os
import generate_limerick
from cStringIO import StringIO
import sys
import random
output_dir = sys.argv[1]

# https://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call
class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout

def create_random_file(limerick):
    rand_filename = "%slimerick_%s.txt" % (output_dir, int(random.random() * 1000000))
    with open(rand_filename, "w+") as f:
        f.seek(0)
        for line in limerick:
            f.write("%s\n" % line)

def generate():
    with Capturing() as output:
        generate_limerick.generate_limerick()

    create_random_file(output)

if __name__ == "__main__":
    generate()
