import zlib

hash_value = zlib.adler32("001".encode())
print(hash_value % (32))
