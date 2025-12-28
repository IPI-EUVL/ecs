def encode(segments : list):
    ret = bytes()

    for s in segments:
        ret += len(s).to_bytes(length=2, byteorder="big")
        ret += s

    return ret

def decode(b : bytes):
    i = 0
    segments = []

    while i < len(b):
        seg_l = int.from_bytes(b[i:i+2], byteorder="big")

        s = b[i+2:i + seg_l + 2]
        segments.append(s)

        i += seg_l + 2

    return segments
