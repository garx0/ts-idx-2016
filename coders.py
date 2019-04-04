from array import array as _array
import numpy as _np

def vb_encode(n_list):
    codes_l = []
    code_l = []
    for n in n_list:
        code_l = [(n & 127) | 128]
        n = n >> 7
        while n:
            code_l.append(n & 127)
            n = n >> 7
        codes_l += code_l[::-1]
    code = _array('B', codes_l)
    return code

def vb_decode(arr):
    n_list = []
    n = 0
    for b in arr:
        n = (n << 7) | (b & 127)
        if b & 128:
            n_list.append(n)
            n = 0
    return n_list

def vb_append(arr, n):
    code_l = [(n & 127) | 128]
    n = n >> 7
    while n:
        code_l.append(n & 127)
        n = n >> 7
    arr.extend(code_l[::-1])

s9_by_selector = {
    # selector: (size, count, waste)
    0: (1, 28, 0),
    1: (2, 14, 0),
    2: (3, 9, 1),
    3: (4, 7, 0),
    4: (5, 5, 3),
    5: (7, 4, 0),
    6: (9, 3, 1),
    7: (14, 2, 0),
    8: (28, 1, 0)
}

s9_by_size = {
    # size: selector, count, waste
    1: (0, 28, 0),
    2: (1, 14, 0),
    3: (2, 9, 1),
    4: (3, 7, 0),
    5: (4, 5, 3),
    7: (5, 4, 0),
    9: (6, 3, 1),
    14: (7, 2, 0),
    28: (8, 1, 0)
}

def s9_encode_word(n_list, selector, size, count, waste):
    word = selector << waste
    for n in n_list[:count]:
        word = (word << size) | n
    return word

def s9_decode_word(word, remove_zeros=False):
    selector = word >> 28
    size, count, waste = s9_by_selector[selector]
    n_list = []
    mask = (1 << size) - 1
    for i in xrange(count):
        n = word & mask
        if n or not remove_zeros:
            n_list.append(n)
        word = word >> size
    return n_list[::-1]

def s9_decode(arr, remove_zeros=False):
    n_list = []
    for word in arr:
        n_list += s9_decode_word(int(word), remove_zeros)
    return n_list

def s9_append(arr, buf, force_clear=False):
    def has_size(n, k):
        return (n >> k) == 0
    buf_start = 0
    sizes_init = [1, 2, 3, 4, 5, 7, 9, 14, 28]
    maxsize_idx = 0
    count = 0
    words = _array('I')
    while(True):
        sizes = sizes_init
        maxsize_idx = 0
        count = 0
        for n in buf[buf_start:]:
            count += 1
            for j, size in enumerate(sizes[maxsize_idx:]):
                if has_size(n, size):
                    maxsize_idx += j
                    break
            max_size = sizes[maxsize_idx]
            selector, need_count, waste = s9_by_size[max_size]
            if count >= need_count:
                word = s9_encode_word(
                    buf[buf_start:buf_start + need_count],
                    selector, max_size, need_count, waste
                )
                words.append(word)
                buf_start += need_count
                break # in worst case we will read 13 numbers again
        else:
            if force_clear:
                word = s9_encode_word(
                    # _np.pad(buf[buf_start:], (0, need_count - count), mode='constant'),
                    buf[buf_start:] + _array('I', [0] * (need_count - count)),
                    selector, max_size, need_count, waste
                )
                words.append(word)
                read_ = len(buf)
            else:
                read_ = buf_start
            arr.extend(words)
            return read_
