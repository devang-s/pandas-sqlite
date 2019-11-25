import sqlite3


class DatabaseConnection:
    def __init__(self):
        self.host = 'waste_adj'
        self.connection = None

    def __enter__(self):
        self.connection = sqlite3.connect(self.host)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb or exc_type or exc_val:
            print("tb:", exc_tb)
            print("type:", exc_type)
            print("value:", exc_val)
            self.connection.close()
        else:
            self.connection.commit()
            self.connection.close()
