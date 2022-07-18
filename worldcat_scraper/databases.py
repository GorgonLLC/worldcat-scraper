import apsw
from scrapy.utils.project import get_project_settings

class WorldcatScraperDatabase:
    dbfile  = get_project_settings().get('SQLITE_FILE')

    def __init__(self):
        self.setupDBCon()
        self.createDDL()

    def setupDBCon(self):
        self.con = apsw.Connection(self.dbfile)
        self.cur = self.con.cursor()

    def closeDB(self):
        self.con.close()

    def __del__(self):
        self.closeDB()

    def createDDL(self):
        print("Creating table: creators")
        sql = """
            CREATE TABLE IF NOT EXISTS books
            (id INTEGER PRIMARY KEY NOT NULL, oclc_id INTEGER NOT NULL UNIQUE, status INTEGER NOT NULL, updated_at TEXT NOT NULL, data JSON)
        """
        self.cur.execute(sql)

        sql = """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_books_on_oclc_id
                ON books(oclc_id);
                CREATE        INDEX IF NOT EXISTS idx_books_on_status
                ON books(status);
                """
        self.cur.execute(sql)

    def dbExecute(self, *args, **kwargs):
        return self.cur.execute(*args, **kwargs)
