import hashlib
import sys
import os
import time
import logging
from logging.handlers import RotatingFileHandler


class Monitor():
    def __init__(self, sourse, replica, log_file):
        self.sourse = sourse
        self.replica = replica

        self.dirs = [] # directories in replica
        self.existing_dirs = [] # directories in sourse
        self.files = [] # files in replica
        self.file_hash = {} # hashes to comapare file content changes
        self.existing_files = [] # files in sourse

        # logger formating
        self.logger = logging.getLogger('Monitor')
        self.logger.setLevel(logging.INFO)
        log_handler = RotatingFileHandler(log_file, maxBytes=35000, backupCount=2)
        formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        formatter.converter = time.gmtime
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)

    def read_replica(self):
        # read initial data
        for root, dirs, files in os.walk(self.replica, topdown=True):
            for name in dirs:
                self.dirs.append(os.path.join(root, name))
            for name in files:
                self.files.append(os.path.join(root, name))
                self.file_hash[os.path.join(root, name)] = self.hash_of_file(os.path.join(root, name))

    def update_content(self):
        for root, dirs, files in os.walk(self.sourse, topdown=True):
            # update directories
            for name in dirs:
                # create dirs
                self.create_dir(os.path.join(root, name)[len(self.sourse):])
            # update files
            for name in files:
                self.update_file(os.path.join(root, name)[len(self.sourse):])

        self.remove_nonexisting()

    def remove_nonexisting(self):
        # remove unexsiting directories
        for path in self.dirs:
            if path not in self.existing_dirs:
                self.remove_dir(path)

        # update dirs data
        temp = self.dirs
        self.dirs = self.existing_dirs
        self.existing_dirs = temp
        self.existing_dirs.clear()

        # remove unexisting files
        for path in self.files:
            if path not in self.existing_files:
                self.remove_file(path)

        # update files data
        temp = self.files
        self.files = self.existing_files
        self.existing_files = temp
        self.existing_files.clear()

    def remove_dir(self, dir_path):
        # if directory was already removed
        if not os.path.exists(dir_path):
            return

        # empty directory
        for root, dirs, files in os.walk(dir_path, topdown=True):
            # remove all inner files, all dirs will be empty
            for name in files:
                os.remove(os.path.join(root, name))
            # remove all inner dirs
            for name in dirs:
                os.rmdir(os.path.join(root, name))

        # delete directory
        os.rmdir(dir_path)
        self.logger.info(f"Removed directory {dir_path} and it's contents")
        print(f"Removed directory {dir_path} and it's contents")

    def create_dir(self, directory):
        dir_path = os.path.join(self.replica, directory)

        if not os.path.isdir(dir_path):
            # if file exists with the same name because of different op order
            if os.path.isfile(dir_path):
                self.remove_file(dir_path)
            # If it doesn't exist, create it
            os.makedirs(dir_path)
            self.dirs.append(dir_path)
            print(f"Created directory {dir_path}")
            self.logger.info(f"Created directory {dir_path}")

        # add path to current dirs
        self.existing_dirs.append(dir_path)

    def update_file(self, file):
        file_path = os.path.join(self.replica, file)
        sourse_file = os.path.join(self.sourse, file)

        new_hash = self.hash_of_file(sourse_file)

        operation = ""

        # if file exists and content is unchanged return
        if os.path.isfile(file_path):
            if self.file_hash[file_path] == new_hash:
                self.existing_files.append(file_path)
                return
            else:
                operation = "Updated"
        else:
            if os.path.isdir(file_path):
                self.remove_dir(file_path)
            operation = "Copied"

        # else create and write file
        f_replica = open(file_path, "w")
        f_sourse = open(sourse_file, "r")
        f_replica.write(f_sourse.read())

        self.existing_files.append(file_path)
        self.file_hash[file_path] = new_hash
        print(f"{operation} file {file_path}")
        self.logger.info(f"{operation} file {file_path}")

    def remove_file(self, file):
        if not os.path.isfile(file):
            return

        os.remove(file)
        self.file_hash[file] = ""
        print(f"Removed file {file}")
        self.logger.info(f"Removed file {file}")

    def hash_of_file(self, file):
        f = open(file, "rb")
        text = f.read()
        f.close()

        hashT = hashlib.md5()
        hashT.update(text)
        hashT = hashT.hexdigest()

        return hashT


def main():
    if len(sys.argv) < 5:
        print(f"Usage: python3 {sys.argv[0]} sourse_path replica_path sync_interval_mins log_file")
        return

    # sync interval in seconds
    sync_intv = float(sys.argv[3]) * 60

    monitor = Monitor(sys.argv[1], sys.argv[2], sys.argv[4])
    monitor.read_replica()

    while True:
        time.sleep(sync_intv)
        monitor.update_content()


if __name__ == "__main__":
    main()
