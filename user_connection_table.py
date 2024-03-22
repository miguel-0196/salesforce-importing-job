from db import get_db

class User_connection_table():
    def __init__(self, user_id, token_info):
        self.user_id = user_id
        self.token_info = token_info

    @staticmethod
    def get_token_info(user_id):
        db = get_db()
        row = db.execute(
            "SELECT token_info FROM user_connection_table WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not row:
            return None

        return row[0]

    @staticmethod
    def update_token_info(user_id, token_info):
        db = get_db()
        cur = db.execute(
            "UPDATE user_connection_table SET token_info = ? WHERE user_id = ?", (token_info, user_id,)
        )
        db.commit()
        return cur.lastrowid

    @staticmethod
    def create(user_id, token_info):
        db = get_db()
        cur = db.execute(
            "INSERT INTO user_connection_table (user_id, token_info)"
            " VALUES (?, ?)",
            (user_id, token_info),
        )
        db.commit()
        return cur.lastrowid
