import string
import random

NUM_LINES = 1111111


def create_test_file():
    with open('test_data.txt', 'w') as f:
        for i in xrange(0, NUM_LINES):
            f.write(''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(1, random.randint(10, 100))))
            f.write('\n')


if __name__ == '__main__':
    create_test_file()
