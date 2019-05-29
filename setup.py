import os


def setup(input_directory='/tmp/input/', result_directory='/tmp/result/'):
    for directory in [input_directory, result_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)


if __name__ == "__main__":
    setup()
