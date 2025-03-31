create or replace function nKrypt_rndm(input_text string)
returns varchar
language python
runtime_version = '3.8'
handler = 'tokenize'
as 
$$
import random
import string

token_map = {}

def generate_token(input_text):
    tok_len = len(input_text)
    return ''.join(random.choices(string.ascii_letters + string.digits, k = tok_len))

def tokenize(input_text):
    if input_text not in token_map:
        token_map[input_text] = generate_token(input_text)
    return token_map[input_text]
$$
;
