import os
import subprocess
import uuid

def encrypt(data):
    return crypto_helper(data, 'encrypt')._contents

def decrypt(data):
    return crypto_helper(data, 'decrypt')._contents

class crypto_helper:
    """Use an external executable to encrypt / decrypt data."""
    def __init__(
        self,
        data,
        command
    ):
        self._data = data
        self._command = command # encrypt, decrypt
        self._dir = 'C:/Cryptography/'
        self._exe = '{0}Cryptography.exe'.format(self._dir)
        self._mask = str(uuid.uuid4())
        self._input_file = self._make_file()
        self._result = self._crypto()
        self._contents = self._retrieve_data()

    def _make_file(self):
        with open('{0}{1}.txt'.format(self._dir, self._mask), 'w') as i:
            name = i.name
            i.write('\n'.join(self._data))
            i.close()
        return name

    def _crypto(self):
        c = {
            'encrypt': '{0} -e -file="{1}"'.format(self._exe, self._input_file),
            'decrypt': '{0} -d -file="{1}"'.format(self._exe, self._input_file)
        }
        subprocess.run(c[self._command])
        return os.remove(self._input_file)

    def _retrieve_data(self):
        c = {
            'encrypt': '{0}{1}_encrypted.txt'.format(self._dir, self._mask),
            'decrypt': '{0}{1}_decrypted.txt'.format(self._dir, self._mask)
        }
        with open(c[self._command], 'r') as o:
            contents = o.read().splitlines()
            o.close()
            os.remove(o.name)
        return contents