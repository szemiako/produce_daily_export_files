import connection
from datetime import datetime
import interface
import encryption_helper
import pandas as pd
from pandas import DataFrame
import numpy as np
import os
import re
import sys
import xml.etree.ElementTree as et
from zipfile import ZipFile

def get_MapStocks():
    tree = et.parse(f'{os.path.dirname(os.path.abspath(__file__))}/MapStocks.xml')
    root = tree.getroot()
    return { i.attrib['GUID04']: i.attrib['GUID05'] for i in root[0] }

def load_tolerances():
    tree = et.parse(f'{os.path.dirname(os.path.abspath(__file__))}/EnvironmentConfiguration.xml')
    root = tree.getroot()
    return { i.attrib['GUID00']: { j.attrib['GUID']: j.attrib['Tolerance'] for j in root[0] if i.attrib['CustomerGuid'] == j.attrib['CustomerGuid']} for i in root[0] }

def zipup(full_path, files):
    """Helper function to zip the resulting files."""
    with ZipFile(full_path, 'w') as output_zip:
        for f in files:
            output_zip.write(f, os.path.basename(f))
            try:
                os.remove(f)
            except:
                print("""{0} | Unable to remove remove: {1}""".format(datetime.now(), f))
                pass
        return output_zip.close()

def make_files(export_parameters):
    """Helper function to make all files in one shot."""
    map_stocks = get_MapStocks()
    h = {}
    stocks = set()
    # If you want to add more file schema/types, do so here. This ties in the sets of files needed to the format.
    has_stocks = { # Doesn't need to have "Stocks" as a type, but included for consistency. The boolean value represents if the file contains GUID06s.
        'L': {
            'Users': False,
            'Accounts': False,
            'Holdings': True,
            'Transactions': True,
            'Stocks': False
        },
        'P2': {
            'Accounts': False,
            'Holdings': True,
            'Stocks': False
        },
        'P3': {
            'Accounts': False,
            'Holdings': True,
            'Stocks': False
        }
    }

    # Wanted to use a map function here, but at worst this is O(5) run time (highest
    # number of file types is five (5) types). Cannot use map because of the way the
    # Stocks file must be built, as we need a unique list of GUID06s from files
    # that contain GUID06s for the query.
    for kind in has_stocks[export_parameters._schema].keys():
        h[kind] = output_file(export_parameters, kind)

        if has_stocks[export_parameters._schema][kind] == True: # Extracting the GUID06s if the file is known to contain GUID06s.
            try:                                                # Use a try/except in case file that would ordinarily contain GUID06s has no rows. (Cannot filter this out elsewhere as Stocks needs to be "created" with no rows.)
                if kind == 'Holdings':
                    h[kind]._records['mapped_GUID06'] = h[kind]._records.HoldingGUID.apply(lambda x: map_stocks.get(x, np.nan))
                    h[kind]._records.GUID06 = h[kind]._records.mapped_GUID06.combine_first(h[kind]._records.GUID06)
                    h[kind]._records = h[kind]._records.drop(columns=['mapped_GUID06'])

                stocks.add(h[kind]._records.GUID06.unique())
            except:
                print("""{0} | No acitivty for {1} (GUID: "{2}") for date {3}.""".format(
                    datetime.now(),
                    export_parameters._broker_name,
                    export_parameters._broker_guid,
                    export_parameters._date
                ))
                sys.exit()

        if not kind == 'Stocks':
            h[kind]._write_records()

    # One part of the design philosophy was to make minimum exceptions from the
    # design pattern for the Stocks file, but it does need to be created separately
    # as the contents are dependent on other files' contents.
    if len(stocks) >= 1: h['Stocks']._contents = h['Stocks']._get_contents('({0})'.format(re.sub(r'\{|\}', '', str(stocks))))

    h['Stocks']._records = h['Stocks']._make_records()
    h['Stocks']._write_records()

    return [h[op]._output_mask for op in h if len(h[op]._contents) > 0] # We return a list of strings so that we can use the zipup

class export_parameters:
    """Base class that allows us to pass along values from interface into connection. Represents the base parameters required to make a set of daily, per-broker export files. Struggled with ordering the variables."""
    def __init__(
        self,
        schema,
        date,
        broker,
        company,
        inactive,
        closed,
        server
    ):
        self._server = connection.queries(server='{0}.domain'.format(server))
        self._schema = schema
        self._date = self._get_date(date)
        self._created = self._get_created()
        self._broker = broker
        self._broker_name = self._get_broker_name()
        self._broker_guid = self._get_broker_guid()
        self._customer_guid = self._get_customer_guid(company)
        self._inactive = bool(inactive)
        self._closed = bool(closed)
        self._delimiter = ','                       # Can be made an external argument/configuration.

    def _get_created(self):
        return datetime.strftime(datetime.now(), '%Y.%m.%d_%H%M%S')

    def _get_date(self, date):
        return datetime.strptime(date, '%Y-%m-%d').strftime('%Y.%m.%d')

    def _get_broker_name(self):
        broker = re.sub('[\\/:*?<>]', '', self._broker) # Required so that we can make files in legacy format that can be saved. Regex pattern represents invalid characters for filenames.
        return re.sub(' ', '_', broker)

    def _get_broker_guid(self):
        self._broker_guid = self._server._execute_select_one(self._server._get_broker_guid(self._broker))
        return self._broker_guid[0].lower()

    def _get_customer_guid(self, company):
        return self._server._execute_select_one(self._server._get_customer_guid(company))[0]
        
# Initially, this was a class that inherited the properties
# of export parameters. However, this caused too much clutter/difficult
# logic to follow, so instead re-designed such that a base set
# of export parameters needs to be created before creating any export
# files.
# 
# In other words, we want all properties of each output_file to be
# the same, so we only need one (1) instance of export parameters. (If we
# used inheritance, each output file would be a different instance than
# the other (e.g., each would have different "datetime created" (self._created))
# values; this gets really bad with server objects and cursors.)
class output_file:
    def __init__(
        self,
        export_parameters,
        kind
    ):
        self._params = export_parameters
        self._kind = kind
        self._output_mask = self._get_output_mask()
        self._contents = self._get_contents()
        self._records = self._make_records()

    def _get_output_mask(self):
        m = {
            'L': '{0}.{1}.{2}.txt'.format(
                self._params._date,
                self._params._broker_name,
                self._kind
            ),
            'P2': '{0}_{1}_{2}_{3}.txt'.format(
                self._params._date,
                self._params._broker_guid,
                self._params._created,
                self._kind
            ),
            'P3': '{0}_{1}_{2}_{3}.txt'.format(
                self._params._date,
                self._params._broker_guid,
                self._params._created,
                self._kind
            )
        }
        return m[self._params._schema]

    # Seems counter-intuitive, but all we are doing is loading a bunch of strings
    # into one dictionary with a bunch of formatted values replaced. We then
    # execute the required query string to return values. We include Stocks here
    # as well for consistency, but we can "initalize" it with no records(with some
    # lambda tricks) and then later pass along a "list" of GUID06s for processing.
    # Stocks are the only "exception" to the design pattern, but can be handled neatly
    # within the pattern itself, without having to carve out too many exceptions.
    def _get_contents(self, guids=None):
        c = {
            'L': {
                'Users': self._params._server.users(
                    self._params._customer_guid,
                    self._params._inactive
                ),
                'Accounts': self._params._server.accounts(
                    self._params._schema,
                    self._params._date,
                    self._params._broker,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Holdings': self._params._server.holdings(
                    self._params._schema,
                    self._params._date,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Transactions': self._params._server.transactions(
                    self._params._date,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Stocks': self._params._server.stocks(
                    self._params._schema,
                    guids
                )
            },
            'P2': {
                'Accounts': self._params._server.accounts(
                    self._params._schema,
                    self._params._date,
                    self._params._broker,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Holdings': self._params._server.holdings(
                    self._params._schema,
                    self._params._date,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Stocks': self._params._server.stocks(
                    self._params._schema,
                    guids)
            },
            'P3': {
                'Accounts': self._params._server.accounts(
                    self._params._schema,
                    self._params._date,
                    self._params._broker,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Holdings': self._params._server.holdings(
                    self._params._schema,
                    self._params._date,
                    self._params._broker_guid,
                    self._params._customer_guid,
                    self._params._inactive,
                    self._params._closed
                ),
                'Stocks': self._params._server.stocks(
                    self._params._schema,
                    guids
                )
            }
        }
        # Note regarding Execute Select All / Execute Select One. Execute Select One
        # uses FetchOne, and is expected to be used many times. Each time, we clear out
        # the cursor. However, in Execute Select All, we want to keep the cursor, so
        # we do not clear it out.
        return self._params._server._execute_select_all(c[self._params._schema][self._kind]._query)

    # Coniditional statement was meant to be an error handler, but was preventing initialization
    # of empty Stocks object, so we have it return "None" so that we can create the DataFrame
    # later.
    def _make_records(self):
        if len(self._contents) > 0:
            self._records = pd.DataFrame.from_records(
                self._contents,
                columns=[column[0] for column in self._params._server._cursor.description] # We cannot do the deviations check first because we need to keep the previous cursor alive to get the description.
            )                                                                              # We use the cursor used within _get_contents to create the column headers of the file
            
            if self._kind == 'Accounts': self._records.AccountNumber = encryption_helper.decrypt(list(self._records.AccountNumber)) # Gets loaded as a string, so no need to convert. Helps keep leading zeroes!
            test = self._test_file_deviation()
            if test[0]:
                return self._records
            else:
                print("""{0} | Export file deviation for {1} (GUID: "{2}") for date {3}.
                           | Previous row count: {4}
                           | Current row count: {5}
                           | Tolerance: {6}""".format(
                        datetime.now(),
                        self._params._broker,
                        self._params._broker_guid,
                        self._params._date,
                        test[1][0],
                        test[1][1],
                        test[1][2]
                    ))
                sys.exit()
        else:
            return None

    def _get_tolerance(self):
        t = load_tolerances()
        return t[self._params._customer_guid.upper()].get(self._params._broker_guid.upper(), None)

    def _get_last_export_size(self):
        return self._params._server._execute_select_one(self._params._server._check_previous_file_count(self._kind, self._params._customer_guid, self._params._broker_guid, self._params._date))[0]

    def _calculate_file_deviation(self, previous_row_count, current_row_count, tolerance=None):
        if previous_row_count > 0:
            deviation = float(abs(previous_row_count - current_row_count) / previous_row_count) * 100
        else:
            return True
        
        if deviation > tolerance:
            return True  # True means deviation, do not proceed.
        else:
            return False # False means no deviation, can proceed.

    def _test_file_deviation(self):
        export_deviations = {
            'Accounts',
            'Holdings'
        }

        tolerance = self._get_tolerance()
        if self._kind not in export_deviations or tolerance is None:
            return True, []
        
        previous = self._get_last_export_size()
        current = len(self._contents)
        deviation = self._calculate_file_deviation(previous, current, int(tolerance))
        if not deviation:
            return True, []
        else:
            return False, [previous, current, tolerance]

    # Similiarly to the above, we use a conditional statement so that we can initialize an
    # empty Stocks file and write values later.
    def _write_records(self):
        if self._records is not None:
            f = open('{0}'.format(self._output_mask), 'w')
            f.write('{0}\n'.format(self._params._delimiter.join(self._records.columns.tolist())))
            self._records.to_csv(
                f,
                quoting=2,
                header=False,
                index=False,
                line_terminator='\n',
                sep=self._params._delimiter,
                date_format='%m/%d/%Y %H:%M:%S %p'
            )
            return f.close() # Close the file once done writing, to minimize i/o errors within zipup method (still happen, but this minimizes them significantly).
        else:
            return None

# Creates the output folder so that files can be written. Also helpful
# to use with zipup. Doesn't really need to be it's own
# class at this point.
class output_folder:
    def __init__(
        self,
        export_parameters
    ):
        self._params = export_parameters
        self._output_mask = self._get_output_mask()

    def _get_output_mask(self):
        m = {
            'L': '{0}_{1}_{2}_{3}.zip'.format(
                self._params._date,
                self._params._broker_name,
                self._params._created,
                'Data'
            ),
            'P2': '{0}_{1}_{2}_{3}-{4}.zip'.format(
                self._params._date,
                self._params._broker_guid,
                self._params._created,
                'Data',
                self._params._server._execute_select_one(self._params._server._get_customer_append(self._params._customer_guid))[0]
            ),
            'P3': '{0}_{1}_{2}_{3}-{4}.zip'.format(
                self._params._date,
                self._params._broker_guid,
                self._params._created,
                'Data',
                self._params._server._execute_select_one(self._params._server._get_customer_append(self._params._customer_guid))[0]
            )
        }
        return m[self._params._schema]

def process(args):
    params = export_parameters(
        args._args.schema,
        args._args.date,
        args._args.broker,
        args._args.company,
        args._args.inactive,
        args._args.closed,
        args._args.server
    )

    files = make_files(params)
    folder = output_folder(params)
    zipup(folder._output_mask, files)

process(interface.args())