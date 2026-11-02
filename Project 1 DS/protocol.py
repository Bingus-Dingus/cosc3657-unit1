
import json
import struct

MAX_SIZE = 1_000_000   # reject anything over ~1 MB so a bad length can't overload the server


def recv_exact(sock, n):
    """Keep calling recv() until we have exactly n bytes."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))   # may return fewer bytes than asked for
        if not chunk:                      # empty means the other side disconnected
            raise ConnectionError("client disconnected")
        data += chunk
    return data


def send_message(sock, message):
    """Turn a dict into JSON bytes, put the length in front, send it all."""
    body = json.dumps(message).encode("utf-8")
    header = struct.pack("!I", len(body))   # !I = 4-byte unsigned int, big-endian (network byte order)
    sock.sendall(header + body)             # sendall keeps sending until every byte is out


def recv_message(sock):
    """Read the 4-byte length, then read that many bytes and turn them back into a dict."""
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    if length > MAX_SIZE:
        raise ValueError(f"message too large: {length} bytes")
    body = recv_exact(sock, length)
    return json.loads(body.decode("utf-8"))