def create_mssql_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS master.dbo.users (
        id VARCHAR(255) PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(255),
        address TEXT,
        post_code VARCHAR(255),
        email VARCHAR(255),
        username VARCHAR(255),
        registered_date VARCHAR(255),
        phone VARCHAR(255),
        picture TEXT
    );
    """)

def insert_mssql_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO master.dbo.users (id, first_name, last_name, gender, address, 
                    post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Error inserting data for {first_name} {last_name}: {e}")
        # Optionally, raise the exception again to handle it further up the call stack
        raise




(
    df.write.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost.database.windows.net:1433;databaseName=master")
    .option("dbtable", "users")
    .option("user", "sa")
    .option("password", "Admin@123")
    .save()
)