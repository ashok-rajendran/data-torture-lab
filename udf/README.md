## UDF Functions for Tokenization

The following files contain UDF functions designed to apply tokenization to secure PHI and PII column values:

- **nKrypt_hash.py**
- **nKrypt_rndm.sql**
- **DeKrypt_Fernet.py**
- **EnKrypt_Fernet.py**

In these UDFs, I have used open-source packages to tokenize and detokenize the data. However, in enterprise projects, Protegrity (an encryption technology company) typically provides UDF functions for applying tokenization.

---

### **nKrypt_hash.py**
This UDF can be used to tokenize a column using either the SHA-256 or MD5 function based on user input. These are oneway tokenization.

**Example:**  
```sql
SELECT nKrypt_hash('IRONMAN', 'MD5'), nKrypt_hash('IRONMAN', 'SHA256');
```
---

### **nKrypt_rndm.sql**
This UDF can be used to tokenize a column with random values, This is a oneway tokenization which cannot be detokenized.

**Example:**  
```sql
SELECT nKrypt_rndm('HULK'), nKrypt_rndm('IRONMAN');
```
---

### **EnKrypt_Fernet.py & DeKrypt_Fernet.py**
These UDFs can be used to tokenize and detokenize a column.

**Example:**  
```sql
SELECT EnKrypt_Fernet('HULK'), DeKrypt_Fernet('IRONMAN');
