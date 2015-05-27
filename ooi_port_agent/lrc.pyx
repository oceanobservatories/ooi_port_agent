

def lrc(data, seed=0):
    """
    Compute a longitudinal redundancy check value
    Implemented in cython for speed...
    :param data: input string
    :param seed: seed value
    :return: lrc value
    """
    cdef unsigned char* bytes = data
    cdef unsigned char byte, value = seed
    cdef int size = len(data)
    for i in xrange(size):
        value ^= bytes[i]
    return value
