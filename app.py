from flask import Flask, jsonify
import snowflake.connector

app = Flask(__name__)

def get_snowflake_version():
    """
    Connects to Snowflake using key-pair authentication, runs a simple query,
    and returns the Snowflake version.
    """
    # Load your private key from the DER file.
    with open('private_key_pk8.der', 'rb') as key_file:
        private_key_data = key_file.read()

    # Establish the connection using your working parameters.
    conn = snowflake.connector.connect(
        user='GPT_SERVICE_USER',
        account='ptb39397',       # Your basic account locator
        region='us-east-1',       # Region matching your OAuth endpoint
        private_key=private_key_data,
        warehouse='ALT_GENERIC',
        database='SCORING_SEGMENTATION_DB',
        schema='SCORING_MODEL_INTEGRATED_OUTCOME',
        login_timeout=30
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    cursor.close()
    conn.close()
    return version[0] if version else "unknown"

@app.route('/version', methods=['GET'])
def version():
    """
    API endpoint to get the current Snowflake version.
    """
    snowflake_version = get_snowflake_version()
    return jsonify({"snowflake_version": snowflake_version})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
