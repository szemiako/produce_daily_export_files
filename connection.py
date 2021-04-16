import pyodbc

# Base class for the SQL database connection.
class base_connection:
    def __init__(
        self,
        server,
        database,
    ):
        self._server = server
        self._database = database
        self._driver = '{SQL Server}'
        self._trusted_connection = 'yes'
        self._connection_parameters = 'Driver={driver}; Server={server}; Database={db}; Trusted_Connection={trusted_connection}'.format(
            driver = self._driver,
            server = self._server,
            db = self._database,
            trusted_connection = self._trusted_connection
        )
        self._connection = pyodbc.connect(self._connection_parameters)
        self._cursor = self._open()

    # Make a new cursor.
    def _open(self):
        return self._connection.cursor()

    # Close the existing cursor.
    def _close(self):
        return self._cursor.close()

    # Create a fresh cursor.
    def _new_cursor(self):
        self._close()
        return self._open()

# Create a class for queries that inherits the Base Connection.
# In general, this is meant to only "hold" the queries needed for execution,
# and inherits the Base Connection so that we can just quickly call
# the Queries class to interact with the databse with all values
# populated.
class queries(base_connection):
    def __init__(
        self,
        server='QA', # Default server.
        database='DB' # Default databse.
    ):
        super().__init__(
            server,
            database
        )

    # Execute a query, but specifically SELECT statements.
    # fetchall() allows us to return the results of our query
    # (through the use of execute) as a list. We want
    # to retain the cursor information, as we will need it again
    # and also we only expect to use Execute Select All once (for
    # each class).
    # Candidate to be moved to Base Connection.
    def _execute_select_all(self, query):
        self._cursor.execute(query)
        return self._cursor.fetchall()

    # In this case, we only need one (1) value, and don't
    # really care for the cursor. To avoid confusion and
    # to clear up memory, we clear the cusor each time
    # upon execution. That isn't really needed, but helps
    # in debugging.
    # Candidate to be moved to Base Connection.
    def _execute_select_one(self, query):
        self._cursor.execute(query)
        results = self._cursor.fetchone()
        self._cursor = self._new_cursor()
        return results

    # Gets the GUID02 for a specific BrokerName.
    def _get_broker_guid(self, broker_name):
        return """
            SELECT TOP (1) GUID
            FROM dbo.Brokers WITH (NOLOCK)
            WHERE Name = '{broker_name}'
        """.format(broker_name=broker_name)
        
    # Gets the GUID00 for a specific CompanyName.
    def _get_customer_guid(self, customer_name):
        return """
            SELECT TOP (1) GUID00
            FROM dbo.Customers WITH (NOLOCK)
            WHERE Name = '{customer_name}'
        """.format(customer_name=customer_name)

    # PwC wanted us to append the zip filenames with a string
    # so that they could discern to which jurisdiction
    # as set of files was applicable. In production, we leveraged
    # the CompanyAddress1 field to do this. ¯\_(ツ)_/¯
    def _get_customer_append(self, customer_guid):
        return """
            SELECT TOP (1) CompanyAddress1
            FROM dbo.Customers WITH (NOLOCK)
            WHERE GUID = '{customer_guid}'
        """.format(customer_guid=customer_guid)

    def _check_previous_file_count(self, kind, customer_guid, broker_guid, date_of_data):
        return """
            SELECT TOP 1 {count}
            FROM [Counts] 
            WHERE 
                [GUID00] = '{customer_guid}' AND 
                [GUID02] = '{broker_guid}' AND 
                [DateTime] < '{date_of_data}' AND
                CHARINDEX('EXPORT', FileIdentifier, 1) > 0
            ORDER BY FileIdentifier DESC
        """.format(count=(lambda x: 'CountHoldings' if x == 'Holdings' else 'CountAccounts')(kind), customer_guid=customer_guid, broker_guid=broker_guid, date_of_data=date_of_data)

    # No inheritance needed. This is a sub-class within queries,
    # Because all it does is deal with Accounts-specfic queries and
    # populates values using format function needed for querying.
    class accounts:
        def __init__(
            self,
            schema,
            date,
            broker_name,
            broker_guid,
            customer_guid,
            inactive,
            closed
        ):
            self._date = date
            self._schema = schema
            self._broker_name = broker_name
            self._broker_guid = broker_guid
            self._customer_guid = customer_guid
            self._inactive = (lambda r: """[Users].[Status] <> 'inactive' AND""" if r else '')(inactive) # Kind of proud of myself for finally figuring out lamda expression syntax (took a while but they're trivial).
            self._closed = (lambda c: """[Accounts].[Closed] IS NULL AND""" if c else '')(closed)
            self._template = """
                SELECT {s} 
                FROM [Accounts] WITH (NOLOCK) 
                    INNER JOIN [Users] WITH (NOLOCK) ON [Accounts].[GUID01] = [Users].[GUID01] 
                    INNER JOIN [Accounts-DailyProcess] WITH (NOLOCK) ON [Accounts].[GUID03] = [Accounts-DailyProcess].[GUID03] 
                WHERE {r} {x}
                    [Accounts].[GUID02] = '{broker_guid}' AND 
                    [Accounts-DailyProcess].[Date] = '{date}' AND 
                    [Users].[GUID00] = '{customer_guid}'
            """ # Template string required to build daily, per-broker export files. S/o Roko developers for the idea.
            self._query = self._build_query()

        def _build_query(self): # Query builder that takes the schema and returns the query needed for output.
            s = {
                'L': self._legacy_columns(),
                'P2': self._minimized_columns(),
                'P3': self._minimized_columns()
            }
            return self._template.format(s=s[self._schema], r=self._inactive, x=self._closed, broker_guid=self._broker_guid, date=self._date, customer_guid=self._customer_guid)

        def _legacy_columns(self):
            return """
                [Accounts].[Column00],
                [Accounts].[Column01],
                [Accounts].[Column02],
                [Accounts].[Column03],
                [Accounts].[Column04],
                [Accounts].[Column05],
                [Accounts].[Column06],
                [Accounts].[Column07],
                {broker_name}' AS BrokerName
            """.format(broker_name=self._broker_name)

        def _minimized_columns(self):
            return """
                [Accounts].[Column00],
                [Accounts].[Column01],
                [Accounts].[Column03],
                '{broker_name}' AS BrokerName, 
                [Accounts].[Column10]
            """.format(broker_name=self._broker_name)

    # Similar thought process and design pattern for holdings.
    # Tried to create a consistent structure so that new files / types
    # can be added relatively simply in the future. Note that
    # we aren't doing anything here, other than building strings! (E.g.,
    # we aren't executing any SELECT statements.)
    class holdings:
        def __init__(
            self,
            schema,
            date,
            broker_guid,
            customer_guid,
            inactive,
            closed
        ):
            self._date = date
            self._schema = schema
            self._broker_guid = broker_guid
            self._customer_guid = customer_guid
            self._inactive = (lambda r: """[Users].[Status] <> 'inactive' AND""" if r else '')(inactive) # Although cool, this can be moved to a class that all schema types inherit.
            self._closed = (lambda c: """[Accounts].[Closed] IS NULL AND""" if c else '')(closed)
            self._template = """
                SELECT {s} 
                FROM [Holdings] WITH (NOLOCK) 
                INNER JOIN [Accounts] WITH (NOLOCK) ON [Holdings].[GUID03] = [Accounts].[GUID03] 
                INNER JOIN [Users] WITH (NOLOCK) ON [Accounts].[GUID01] = [Users].[GUID01] 
                CROSS APPLY (
                    SELECT TOP (1)
                        ca_pdp.Column00,
                        ca_pdp.Column01,
                        ca_pdp.Column02
                    FROM [Positions-DailyProcess] ca_pdp WITH (NOLOCK)
                    WHERE
                        ca_pdp.GUID04 = [Holdings].[GUID04]
                        AND ca_pdp.Balance IS NOT NULL
                        AND ca_pdp.Balance <> 0
                    ORDER BY ca_pdp.Date DESC
                ) pdp
                {frd}
                WHERE {r} {x}
                    [Accounts].[GUID02] = '{broker_guid}' AND 
                    [Users].[GUID00] = '{customer_guid}'
            """
            self._query = self._build_query()

        def _build_query(self):
            s = {
                'L': self._legacy_columns(),
                'P2': self._minimized_columns_p2(),
                'P3': self._minimized_columns_p3()
            }
            first_report_date_template = """
                CROSS APPLY (
                    SELECT
                        ca_pdp.Column00,
                        MIN(ca_pdp.Column01) [FirstReportDate]
                    FROM [Positions-DailyProcess] ca_pdp WITH (NOLOCK)
                    WHERE
                        ca_pdp.GUID04 = [Holdings].[GUID04]
                    GROUP BY
                        ca_pdp.GUID04
                ) frd
            """
            return self._template.format(s=s[self._schema], r=self._inactive, x=self._closed, frd=(lambda s: first_report_date_template if s == 'P3' else '')(self._schema), broker_guid=self._broker_guid, date=self._date, customer_guid=self._customer_guid)

        def _legacy_columns(self):
            return """
                [Holdings].[Column00], 
                [Holdings].[Column01], 
                [Holdings].[Column02], 
                [Holdings].[Column03], 
                pdp.[Column00] AS 'Shares', 
                pdp.[Column01], 
                [Holdings].[Column04], 
                [Holdings].[Column05]
            """

        def _minimized_columns_p2(self):
            return """
                [Holdings].[Column00], 
                [Holdings].[Column01], 
                [Holdings].[Column02]
            """

        def _minimized_columns_p3(self):
            return """
                {p2},
                frd.FirstReportDate AS FirstReportDate
            """.format(p2=self._minimized_columns_p2())

    # Users template, only exists in Legacy format.
    class users:
        def __init__(
            self,
            customer_guid,
            inactive,
            schema='L'
        ):
            self._schema = schema
            self._customer_guid = customer_guid
            self._inactive = (lambda r: """[Users].[Status] <> 'inactive' AND""" if r else '')(inactive) # Although the argument was added as a boolean, it's stored as a text value. Wonderful.
            self._query = self._build_query() # Not required, but want to keep consistent design pattern.

        def _build_query(self):
            return """
                SELECT 
                    [Column00], 
                    [Column01], 
                    [Column02], 
                    [Column03], 
                    [Column04], 
                    [Column05], 
                    [Column06], 
                    [Column07], 
                    [Column08]
                FROM [Users] WITH (NOLOCK) 
                WHERE {r}
                [GUID00] = '{customer_guid}'
            """.format(r=self._inactive, customer_guid=self._customer_guid)

    # Transactions template, only exists in Legacy format.
    class transactions:
        def __init__(
            self,
            date,
            broker_guid,
            customer_guid,
            inactive,
            closed,
            schema='L'
        ):
            self._date = date
            self._schema = schema
            self._broker_guid = broker_guid
            self._customer_guid = customer_guid
            self._inactive = (lambda r: """[Users].[Status] <> 'inactive' AND""" if r else '')(inactive)
            self._closed = (lambda c: """[Accounts].[Closed] IS NULL AND""" if c else '')(closed)
            self._query = self._build_query()

        def _build_query(self):
            return """
                SELECT 
                    [Trades].[Column01], 
                    [Trades].[Column02], 
                    [Trades].[Column03], 
                    [Trades].[Column04],
                    [Trades].[Column05],
                    [Trades].[Column06], 
                    [Trades].[Column07], 
                    [Trades].[Column08], 
                    [Trades].[Column09], 
                    [Trades].[Column10], 
                    [Trades].[Column11], 
                    [Trades].[Column12]
                FROM [Trades] WITH (NOLOCK) 
                    INNER JOIN [Holdings] WITH (NOLOCK) ON [Trades].[GUID04] = [Holdings].[GUID04] 
                    INNER JOIN [Accounts] WITH (NOLOCK) ON [Holdings].[GUID03] = [Accounts].[GUID03] 
                    INNER JOIN [Users] WITH (NOLOCK) ON [Accounts].[GUID01] = [Users].[GUID01] 
                WHERE {r} {x}
                    [Accounts].[GUID02] = '{broker_guid}' AND 
                    [Trades].[Date] = '{date}' AND 
                    [Users].[GUID00] = '{customer_guid}'
            """.format(r=self._inactive, x=self._closed, broker_guid=self._broker_guid, date=self._date, customer_guid=self._customer_guid)

    class stocks:
        def __init__(
            self,
            schema,
            stock_guids
        ):
            self._schema = schema
            self._stock_guids = (lambda s: s if s is not None else """(NEWID())""")(stock_guids) # Extension of the lambda expressions above. Cannot be True / False, as stock_guids are not boolean (they are expected to be a list).
            self._template = """
                SELECT
                    [Stocks].[Column00],
                    [Stocks].[Column01],
                    [Stocks].[Column02],
                    [Stocks].[Column03],
                    CASE
                        WHEN [Stocks].[Type] = X
                            THEN ''
                        WHEN [Stocks].[Type] = Y
                            AND Column04 LIKE '[A-Z]%[0-9]%'
                            AND CreatedBy IN (SELECT Name FROM Brokers WHERE CAST(Country AS varchar(MAX)) = 'USA')
                            THEN ''
                        ELSE [Stocks].[Column04]
                    END [Column04],
                    {osi}
                    {s}
                FROM [Stocks] WITH (NOLOCK)
                INNER JOIN [Types] WITH (NOLOCK) ON [Stocks].[Type] = [Types].[Type]
                WHERE [Stocks].[GUID06] IN {stock_guids}
            """
            self._query = self._build_query()
            
        def _build_query(self):
            s = {
                'L': self._legacy_columns(),
                'P2': self._minimized_columns_p2(),
                'P3': self._minimized_columns_p3()
            }
            return self._template.format(osi = (lambda o: '' if o == 'P3' else "'' AS OSISymbolFormat,")(self._schema), s=s[self._schema], stock_guids=self._stock_guids)

        def _legacy_columns(self):
            return """
                [Stocks].[Column05],
                [Stocks].[Column06]
            """

        def _minimized_columns_p2(self):
            return """
                CASE
                    WHEN [Stocks].[Type] = X
                        THEN 'IDENTIFIER 1'
                    WHEN [Stocks].[Type] = Y
                        AND Column04 LIKE '[A-Z]%[0-9]%'
                        AND CreatedBy IN (SELECT Name FROM Brokers WHERE CAST(Country AS varchar(MAX)) = 'USA')
                        THEN 'IDENTIFIER 2'
                    ELSE ''
                END [Column07],
                CASE
                    WHEN [Stocks].[Type] = 35
                        THEN [Stocks].[Column08]
                    WHEN [Stocks].[Type] = 12
                        AND Column04 LIKE '[A-Z]%[0-9]%'
                        AND CreatedBy IN (SELECT Name FROM Brokers WHERE CAST(Country AS varchar(MAX)) = 'USA')
                        THEN [Stocks].[Column04]
                    ELSE ''
                END [Column08],
                COALESCE( NULLIF( LTRIM( RTRIM([Stocks].[Column09])),''), [Stocks].[Column10]) AS Column09,
                COALESCE( NULLIF( LTRIM( RTRIM([Stocks].[Column10])),''), [Stocks].[Column09]) AS Column10
            """

        def _minimized_columns_p3(self):
            return """
                {p2},
                ISNULL([Stocks].[Column11], 1) [Column11]
            """.format(p2=self._minimized_columns_p2())