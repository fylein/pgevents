import brotli

def compress(string):
    return brotli.compress(str.encode(string))

def decompress(bytes):
    return brotli.decompress(bytes).decode()
