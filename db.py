from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert(table, data):
    return supabase.table(table).upsert(data).execute()

def fetch_last_n(table, column, n):
    response = (
        supabase.table(table)
        .select(column)
        .order("trade_date", desc=True)
        .limit(n)
        .execute()
    )

    if not response.data:
        return []

    return [row[column] for row in response.data]