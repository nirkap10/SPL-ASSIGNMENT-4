import atexit
import sqlite3

from DAO import _Vaccines, _Clinics, _Suppliers, _Logistics


class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE vaccines (
            id              INTEGER     PRIMARY KEY,
            date            DATE        NOT NULL,
            supplier        INTEGER     REFERENCES Supplier(id),  
            quantity        INTEGER     NOT NULL
        );

        CREATE TABLE suppliers (
            id              INTEGER     PRIMARY KEY,
            name            STRING      NOT NULL,
            logistic        INTEGER     REFERENCES Logistic(id)
        );

        CREATE TABLE clinics (
            id              INTEGER     PRIMARY KEY,
            location        STRING      NOT NULL,
            demand          INTEGER     NOT NULL,
            logistic        INTEGER     REFERENCES Logistic(id)
        );
        
        CREATE TABLE logistics (
            id              INTEGER     PRIMARY KEY,
            name            STRING      NOT NULL,
            count_sent      INTEGER     NOT NULL,
            count_received  INTEGER     NOT NULL
        );

    """)


# the repository singleton
dbcon = _Repository()
atexit.register(dbcon._close)
