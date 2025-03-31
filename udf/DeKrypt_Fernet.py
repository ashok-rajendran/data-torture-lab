create or replace function DeKrypt_Fernet(input_text string)
returns string
language python
runtime_version = '3.8'
handler = 'detokenize'
packages = ('cryptography')
as
$$
from cryptography.fernet import Fernet

encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

def detokenize(input_text):
  return cipher.decrypt(input_text.encode()).decode()
$$
