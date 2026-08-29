from services.database_service import DatabaseService

db = DatabaseService(
    host="localhost",
    port=5432,
    database="supply_chain",
    user="postgres",
    password="postgres",
)