from db import get_db

class Importing_job_table():
    def __init__(self, user_id, token_info):
        self.user_id = user_id
        self.token_info = token_info

    @staticmethod
    def get_row(user_id, object_name):
        db = get_db()
        row = db.execute(
            "SELECT * FROM importing_job_table WHERE user_id = ? and object_name = ?", (user_id, object_name)
        ).fetchone()
        return row

    @staticmethod
    def update_row(user_id, object_name, start_date, last_date = '0000-00-00', active = 1):
        db = get_db()
        cur = db.execute(
            "UPDATE importing_job_table SET start_date = ?, last_date = ?, active = ? WHERE user_id = ? and object_name = ?", (start_date, last_date, active, user_id, object_name)
        )
        db.commit()
        return cur.lastrowid

    @staticmethod
    def create(user_id, object_name, start_date, last_date = '0000-00-00', active = 1):
        db = get_db()
        cur = db.execute(
            "INSERT INTO importing_job_table (user_id, object_name, start_date, last_date, active)"
            " VALUES (?, ?, ?, ?, ?)",
            (user_id, object_name, start_date, last_date, active),
        )
        db.commit()
        return cur.lastrowid
