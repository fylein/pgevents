import brotli

def compress(string):
    return brotli.compress(string=str.encode(string), mode=brotli.MODE_TEXT, quality=5)

def decompress(bytes):
    return brotli.decompress(bytes).decode()
