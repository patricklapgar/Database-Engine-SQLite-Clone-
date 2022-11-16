import os
import unittest
from subprocess import run

class DBTests(unittest.TestCase):
    def __init__(self) -> None:
        self.output = []
        # self.tests = []
    
    def run(self, commands):
        """
        Helper function to send a list of commands to the database program
        """

        # Start the db program
        # os.system("./db")
        run(["./db", "r+"])
        for command in commands:
            output = run(command, capture_output=True).stdout
            self.output.append(output)

    def test(self, results):
        for idx, command in enumerate(self.output):
            self.assertEquals(command, results[idx])
            
        print("Done")

if __name__ == "__main__":
    db = DBTests()

    # Some rudimentary unit testing
    tests = [
        "insert 1 user1 person1@example.com",
        "select",
        "insert 2 user2 person2@example.com",
        "select",
        ".exit"
    ]

    expected_results = [
        "db > Executed",
        "db > (1, user1, person1@example.com)",
        "db > Executed",
        "db > (1, user1, person1@example.com)\n(2, user2, person2@exmample.com)"
        "db >"
    ]

    db.run(tests)
    db.test(expected_results)
