create or replace function EnKrypt_Fernet(input_text string)
returns string
language python
runtime_version = '3.8'
handler = 'tokenize'
packages = ('cryptography')
as 
$$
from cryptography.fernet import Fernet

encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

def tokenize(input_text):
  return cipher.encrypt(input_text.encode()).decode()
$$
