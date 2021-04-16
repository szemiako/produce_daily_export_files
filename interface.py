import argparse

# Imagine taking functional code to make command-line arguments
# for Python scripts, and then making it class-based. This is
# the idea behind Args. We do this so that we can quickly add new
# command-line parameters with ease in the future.
class args:
    def __init__(
        self
    ):
        self._parser = argparse.ArgumentParser(
            description="""
            This script is intended to generate daily, export files,
            in legacy, phase 2, or phase 3 schema.
        """,
            add_help=True
        )
        self._schema = self._add_param('-s')
        self._date = self._add_param('-d')
        self._broker = self._add_param('-b')
        self._company = self._add_param('-c')
        self._inactive = self._add_param('-r')
        self._closed = self._add_param('-x')
        self._server = self._add_param('-v')
        self._args = self._parser.parse_args()

    def _add_param(self, param):
        params = {
            '-d': args.date(),
            '-s': args.schema(),
            '-b': args.broker(),
            '-c': args.company(),
            '-r': args.inactive(),
            '-x': args.closed(),
            '-v': args.server()
        }
        p = params[str(param)]

        return self._parser.add_argument(
            p._option1,
            p._option2,
            help=p._help,
            type=p._type,
            choices=p._choices,
            required=p._required,
            default=p._default
        )
        
    class date:
        def __init__(
            self,
            o1='-d',
            o2='--date',
            h='-d, --date: The desired date for reprocessing. Generally, this is the desired data date. Must be in YYYY-MM-DD format.',
            t=str,
            c=None,
            r=True,
            d=None
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices = c
            self._required = r
            self._default = d

    class schema:
        def __init__(
            self,
            o1='-s',
            o2='--schema',
            h='-s, --schema: Legacy data minimized format (L), PII-free data-minimized format phase 2 (P2), PII-free data-minimized format phase 3 (P3).',
            t=str,
            c=[
                'L',
                'P2',
                'P3'
            ],
            r=True,
            d='L'
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices= c
            self._required = r
            self._default = d
           
    class broker:
        def __init__(
            self,
            o1='-b',
            o2='--broker',
            h='-b, --broker: The name of the brokerage. Must match a value provided within the database.',
            t=str,
            c=None,
            r=True,
            d=None
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices= c
            self._required = r
            self._default = d

    class company:
        def __init__(
            self,
            o1='-c',
            o2='--company',
            h='-c, --company: The company name, as provided within the database.',
            t=str,
            c=None,
            r=True,
            d=None
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices= c
            self._required = r
            self._default = d

    class inactive:
        def __init__(
            self,
            o1='-r',
            o2='--inactive',
            h='',
            t=bool,
            c=None,
            r=False,
            d=True
        ):
            self._option1 = o1
            self._option2 = o2
            self._type = t
            self._choices= c
            self._required = r
            self._default = d
            self._help = '-r, --inactive: If set to True, then do not include data for users with inactive status. Default is set to: {0}'.format(self._default)

    class closed:
        def __init__(
            self,
            o1='-x',
            o2='--closed',
            h='',
            t=bool,
            c=None,
            r=False,
            d=True
        ):
            self._option1 = o1
            self._option2 = o2
            self._type = t
            self._choices= c
            self._required = r
            self._default = d
            self._help = '-x, --closed: If set to True, then do not include data for accounts with closed status. Default is set to: {0}'.format(self._default)

    class server:
        def __init__(
            self,
            o1='-v',
            o2='--server',
            h='',
            t=str,
            c=[
                'us_production_1',
                'us_production_2'
            ],
            r=False,
            d='QA'
        ):
            self._option1 = o1
            self._option2 = o2
            self._type = t
            self._choices = c
            self._required = r
            self._default = d
            self._help = '-v, --server: The database server that hosts the customer. Default is set to: {0}'.format(self._default)