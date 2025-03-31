create or replace function nKrypt_hash(input_text string, type string)
returns string
language python
runtime_version = '3.8'
handler = 'tokenize'
as 
$$
import hashlib
def tokenize(input_text,type):
    if type == 'sha256':
        return hashlib.sha256(input_text.encode()).hexdigest() if input_text else None
    elif type == 'MD5':
        return hashlib.md5(input_text.encode()).hexdigest() if input_text else None
$$
;
