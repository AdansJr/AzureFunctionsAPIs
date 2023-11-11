import time
import json
import pyodbc
import textwrap
import logging
import azure.functions as func

from configparser import ConfigParser
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    config_parser = ConfigParser()
    try:
        req_body = req.get_json()
    except: pass
    table_name = req_body.get('table_name')

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")

    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
    
    # Define the Query.
    upsert_query = textwrap.dedent("""
    SELECT * FROM [dbo].[{table_name}]
    """.format(table_name=table_name))
    
    cursor_object.execute(upsert_query)
    
    query = []
    
    for i in cursor_object.fetchall():
        query.append(i)
    
    print(query)
    
    columns = [column[0] for column in cursor_object.description]
    print(columns)

    results = []
    for row in query:
        results.append(dict(zip(columns,row)))
        
    print(results)
    logging.info(msg='Query Successful.')
        
    # Return the Response.
    return func.HttpResponse(
            body=json.dumps(results, indent=4),
            status_code=200
        )
    
    

   
    

@app.route(route="newuser", auth_level=func.AuthLevel.ANONYMOUS)
def newuser(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
            req_body = req.get_json()
    except ValueError:
            return func.HttpResponse(
                status_code=400,
                body='Bad Request'
            )

    config_parser = ConfigParser()

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")
	
    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
	
	# Req Params from Json Body
    nome = req_body.get('Nome')
    senha = req_body.get('Senha')


    # Define an Insert Query
    insert_sql = "INSERT INTO [usuarios] (nome, senha) VALUES (?, ?);"


	# Define my Recordset
    records = [
        (nome, senha),
    ]

    # Define the Data Types of the Input Values
    cursor_object.setinputsizes(
        [
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
        ]
    )


    # Execute the Insert Statement
    cursor_object.executemany(insert_sql, records)

    # Commit the Transaction
    cursor_object.commit()

    # Close the connection
    cnxn.close()
    
    details = {
        "Details":"New [User] was succefuly added."
    }
	
    return func.HttpResponse(body=json.dumps(details, indent=4),
                             status_code=200)
    
    

    
    
    
@app.route(route="getuserid", auth_level=func.AuthLevel.ANONYMOUS)
def getuserid(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
            req_body = req.get_json()
    except ValueError:
            return func.HttpResponse(
                status_code=400,
                body='Bad Request'
            )

    config_parser = ConfigParser()

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")
	
    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
	
	# Req Params from Json Body
    nome = req_body.get('Nome')

    upsert_query = textwrap.dedent("""
    SELECT id, nome
    FROM [usuarios]
    WHERE nome = '{nome}';
    """.format(nome=nome))
    
    cursor_object.execute(upsert_query)
    
    query = []
    
    for i in cursor_object.fetchall():
        query.append(i)
    
    print(query)
    
    columns = [column[0] for column in cursor_object.description]
    print(columns)
    results = []
    
    for row in query:
        results.append(dict(zip(columns,row)))
        
    print(results)
    # Close the connection
    cnxn.close()
    
    # Return the Response.
    return func.HttpResponse(
            body=json.dumps(results, indent=4),
            status_code=200
        )
    
    
 
    
    
    
@app.route(route="postbalanca", auth_level=func.AuthLevel.ANONYMOUS)
def postbalanca(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
            req_body = req.get_json()
    except ValueError:
            return func.HttpResponse(
                status_code=400,
                body='Bad Request'
            )

    config_parser = ConfigParser()

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")
	
    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
	
	# Req Params from Json Body
    nome_balanca = req_body.get('nome_balanca')
    pesagem_op = req_body.get('pesagem_op')
    tipo_massa = req_body.get('tipo_massa')
    limp_5s = req_body.get('limp_5s')
    observacao = req_body.get('observacao')
    bal_user = req_body.get('bal_user')


    # Define an Insert Query
    insert_sql = textwrap.dedent("""
                                 INSERT INTO balanca (nome_balanca, pesagem_op, tipo_massa, limp_5s, observacao, bal_user)
                                 VALUES (?, ?, ?, ?, ?, ?);
                                 """)

	# Define my Recordset
    records = [
        (nome_balanca, pesagem_op, tipo_massa, limp_5s, observacao, bal_user)
    ]

    # Define the Data Types of the Input Values
    cursor_object.setinputsizes(
        [
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER)
        ]
    )

    # Execute the Insert Statement
    cursor_object.executemany(insert_sql, records)

    # Commit the Transaction
    cursor_object.commit()

    # Close the connection
    cnxn.close()
    details = {
        "Details":"New [balanca] was succefuly added."
    }
    return func.HttpResponse(json.dumps(details, indent=4),
                             status_code=200)
    
    
   
    
    
    
@app.route(route="postbanbury", auth_level=func.AuthLevel.ANONYMOUS)
def postbanbury(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
            req_body = req.get_json()
    except ValueError:
            return func.HttpResponse(
                status_code=400,
                body='Bad Request'
            )

    config_parser = ConfigParser()

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")
	
    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
	
	# Req Params from Json Body
    nome_banbury = req_body.get('nome_banbury')
    mistura_pop = req_body.get('mistura_pop')
    tipo_massa = req_body.get('tipo_massa')
    limp_5s = req_body.get('limp_5s')
    observacao = req_body.get('observacao')
    ban_user = req_body.get('ban_user')


    # Define an Insert Query
    insert_sql = textwrap.dedent("""
                                 INSERT INTO banbury (nome_banbury, mistura_pop, tipo_massa, limp_5s, observacao, ban_user)
                                 VALUES (?, ?, ?, ?, ?, ?);
                                 """)

	# Define my Recordset
    records = [
        (nome_banbury, mistura_pop, tipo_massa, limp_5s, observacao, ban_user)
    ]

    # Define the Data Types of the Input Values
    cursor_object.setinputsizes(
        [
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER)
        ]
    )

    # Execute the Insert Statement
    cursor_object.executemany(insert_sql, records)

    # Commit the Transaction
    cursor_object.commit()

    # Close the connection
    cnxn.close()
    details = {
        "Details":"New [banbury] was succefuly added."
    }
    return func.HttpResponse(json.dumps(details, indent=4),
                             status_code=200)
    
  
    
    
    
@app.route(route="postextrusora", auth_level=func.AuthLevel.ANONYMOUS)
def postextrusora(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
            req_body = req.get_json()
    except ValueError:
            return func.HttpResponse(
                status_code=400,
                body='Bad Request'
            )

    config_parser = ConfigParser()

    # Load the Database Credentials.
    config_parser.read('config.ini')
    database_username = config_parser.get('baseintegrador', 'data_username')
    database_password = config_parser.get('baseintegrador', 'data_password')
    database = config_parser.get('baseintegrador', 'database')
    database_server = config_parser.get('baseintegrador', 'server')

    logging.info('Loaded Database Credentials.')
	
    # Grab the Drivers.
    logging.info(pyodbc.drivers())

    # Define the Driver.
    driver = '{ODBC Driver 18 for SQL Server}'

    # Create the connection String.
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={database_server}.database.windows.net,1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=database_username,
        password=database_password,
        database_server=database_server,
        database=database,
    )).replace("'", "")
	
    # Create a new connection.
    try:
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    except pyodbc.OperationalError:
        time.sleep(2)
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

    logging.info(msg='Database Connection Successful.')

    # Create the Cursor Object.
    cursor_object: pyodbc.Cursor = cnxn.cursor()
	
	# Req Params from Json Body
    nome_extrusora = req_body.get('nome_extrusora')
    temp_tunel = req_body.get('temp_tunel')
    temp_coxo = req_body.get('temp_coxo')
    espessura = req_body.get('espessura')
    diametro_interno = req_body.get('diametro_interno')
    tipo_massa = req_body.get('tipo_massa')
    limp_matriz = req_body.get('limp_matriz')
    limp_5s = req_body.get('limp_5s')
    observacao = req_body.get('observacao')
    ext_user = req_body.get('ext_user')
    
    # Define an Insert Query
    insert_sql = textwrap.dedent("""
                                 INSERT INTO extrusora (nome_extrusora, temp_tunel, temp_coxo, espessura, diametro_interno, tipo_massa, limp_matriz, limp_5s, observacao, ext_user)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                 """)

	# Define my Recordset
    records = [
        (nome_extrusora, temp_tunel, temp_coxo, espessura, diametro_interno, tipo_massa, limp_matriz, limp_5s, observacao, ext_user)
    ]

    # Define the Data Types of the Input Values
    cursor_object.setinputsizes(
        [
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER),
            (pyodbc.SQL_WVARCHAR, 50, 0),
            (pyodbc.SQL_INTEGER)
        ]
    )

    # Execute the Insert Statement
    cursor_object.executemany(insert_sql, records)

    # Commit the Transaction
    cursor_object.commit()

    # Close the connection
    cnxn.close()
    details = {
        "Details":"New [extrusora] was succefuly added."
    }
    return func.HttpResponse(json.dumps(details, indent=4),
                             status_code=200)