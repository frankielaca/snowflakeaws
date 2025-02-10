import snowflake.connector

print("Starting full connection test...")

# Load your private key from the DER file.
with open('private_key_pk8.der', 'rb') as key_file:
    private_key_data = key_file.read()

try:
    conn = snowflake.connector.connect(
        user='GPT_SERVICE_USER',
        account='ptb39397',     # Basic account locator
        region='us-east-1',     # Specify the us-east-1 region
        private_key=private_key_data,
        warehouse='ALT_GENERIC',           
        database='SCORING_SEGMENTATION_DB',
        schema='SCORING_MODEL_INTEGRATED_OUTCOME',
        login_timeout=30
    )
    print("Full connection test succeeded!")
    
    # Execute a simple query to verify connectivity.
    cursor = conn.cursor()
    print("Executing query: SELECT CURRENT_VERSION()")
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    print("Snowflake version:", version)
    
    cursor.close()
    conn.close()
    print("Connection closed. Script finished.")
except Exception as e:
    print("Full connection test failed:", e)
