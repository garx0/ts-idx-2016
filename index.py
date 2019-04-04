#!/usr/bin/env python
from __future__ import print_function
import argparse
import document_pb2
import struct
import gzip
import sys

import doc2words
import mmh3
from collections import defaultdict
from array import array
from coders import *
# from datetime import datetime

# buffer will be passed to simple9 encoder only if len(buffer) is in buf_len_thres.
# choosing of this parameter will reduce count of size-measurings of the same number
# (because there may be not enough numbers to encode into 4-byte word with simple9)
buf_len_thres = {14*6}

class DocumentStreamReader:
    def __init__(self, paths):
        self.paths = paths

    def open_single(self, path):
        return gzip.open(path, 'rb') if path.endswith('.gz') else open(path, 'rb')

    def __iter__(self):
        for path in self.paths:
            with self.open_single(path) as stream:
                while True:
                    sb = stream.read(4)
                    if sb == '':
                        break

                    size = struct.unpack('i', sb)[0]
                    msg = stream.read(size)
                    doc = document_pb2.document()
                    doc.ParseFromString(msg)
                    yield doc


def parse_command_line():
    parser = argparse.ArgumentParser(description='compressed documents reader')
    parser.add_argument('files', nargs='+', help='Input files (.gz or plain) to process')
    parser.add_argument('-c', '--coding', type=str,
                        action='store', dest='coding', choices=['varbyte', 'simple9'],
                        help='Coding method (varbyte | simple9)')
    return parser.parse_args()

def arr_size_count(arr):
    return arr.buffer_info()[1] * arr.itemsize

def mmh3_hash(s):
    """
    64-bit hash
    """
    return mmh3.hash64(s)[0]

if __name__ == '__main__':
    # t1 = datetime.now()
    index = {}
    docs = []
    reader = DocumentStreamReader(parse_command_line().files)
    coding = parse_command_line().coding
    if coding is None:
        coding = 'varbyte'
    doc_id = 1
    for doc in reader:
        # print("{:<7} ({:>6} B): {}".format(doc_id, len(doc.text), doc.url))
        words = doc2words.extract_words(doc.text)
        docs.append(doc.url.encode('utf-8'))
        if coding == 'varbyte':
            for word in words:
                word = word.encode('utf-8')
                mm_hash = mmh3_hash(word)
                if mm_hash not in index:
                    # index[word] = (postlist, last doc_id)
                    #    (for easier appending to postlist)
                    index[mm_hash] = [vb_encode([doc_id]), doc_id]
                else:
                    w_pair = index[mm_hash]
                    if w_pair[1] != doc_id:
                        vb_append(w_pair[0], doc_id - w_pair[1])
                        w_pair[1] = doc_id
        elif coding == 'simple9':
            for word in words:
                word = word.encode('utf-8')
                mm_hash = mmh3_hash(word)
                if mm_hash not in index:
                    # index[word] = (postlist, last doc_id, buffer_for_s9)
                    index[mm_hash] = [array('I'), doc_id, array('I', [doc_id])]
                else:
                    w_pair = index[mm_hash]
                    buf = w_pair[2]
                    if w_pair[1] != doc_id:
                        buf.append(doc_id - w_pair[1])
                        w_pair[1] = doc_id
                        if len(buf) in buf_len_thres:
                            read_ = s9_append(w_pair[0], buf)
                            buf[:] = buf[read_:]
        doc_id += 1
    # t2 = datetime.now()
    # time_elapsed = (t2 - t1).total_seconds()
    # print("indexing done in {} s (except force_clears in s9)".format(time_elapsed))
    # t1 = datetime.now()

    # index is ready. serializing it:

    # index file:
    #   (everything in file is aligned by 4-byte words, even varbyte codes)
    #
    #   comp_type / num_buckets; (4 bytes,
    #       first bit is for post_list compression type (0: varbyte, 1: simple9))
    #   bucket_sizes_array; (num_buckets * 2 bytes)
    #   {bucket_1}; ... ; {bucket_N};
    # bucket_i: (bucket_size is full size of this structure in file)
    #   num_hashes; (4 bytes)
    #   hash_1; (8 bytes)
    #   post_list_1_size; (4 bytes) NOTE: this is the size of encoding.
    #                               actual size in file =
    #                               aligned(this) = (this)+(-this)%4
    #   post_list_1; (aligned(post_list_1_size))
    #   ...
    #   hash_m;  (8 bytes)
    #   post_list_m_size; (4 bytes)
    #   post_list_m; (aligned(post_list_m_size))

    # docs file:
    # num_buckets; (4 bytes)
    # bucket_sizes_array; (bucket_sizes_array_size * 2 bytes)
    # {bucket_1}; ...; {bucket_M}
    # bucket_i:
    #   n_docs; (4 bytes)
    #   doc_1_id; (4 bytes)
    #   doc_1_url_size; (4 bytes)
    #   doc_1_url; (doc_1_url_size)
    #   ...
    #   doc_D_id; (4 bytes)
    #   doc_D_url_size; (4 bytes)
    #   doc_D_url; (doc_D_url_size)


    n_buckets = 2**16 # must be power of 2, but < 2**32

    buckets = [[] for i in xrange(n_buckets)]
    bucket_sizes = array('I', [4 for i in xrange(n_buckets)]) # 4 -- for 'num_hashes'
    for mm_hash, item in index.items():
        arr = item[0]
        if coding == 'simple9':
            buf = item[2]
            if len(buf):
                s9_append(arr, buf, force_clear=True)
                buf[:] = buf[0:0]
        bucket_n = mm_hash & (n_buckets - 1)
        bucket = buckets[bucket_n]
        arr_size = arr_size_count(arr)
        bucket.append([mm_hash, arr_size, arr])
        bucket_sizes[bucket_n] += 8 + 4 + arr_size + (-arr_size) % 4

    if coding == 'varbyte':
        comp_nbuc = n_buckets
    elif coding == 'simple9':
        comp_nbuc = n_buckets | (1 << 31)

    with open('index', 'w') as f:
        array('I', [comp_nbuc]).write(f)
        array('H', bucket_sizes).write(f)
        for bucket in buckets:
            num_hashes = len(bucket)
            array('I', [num_hashes]).write(f)
            if num_hashes:
                for (mm_hash, arr_size, arr) in bucket:
                    array('l', [mm_hash]).write(f)
                    array('I', [arr_size]).write(f)
                    arr.write(f)
                    if coding == 'varbyte':
                        align_remain = (-arr_size) % 4
                        if align_remain:
                            array('B', [0] * align_remain).write(f)
    del buckets
    del bucket_sizes
    n_doc_buckets = 2**12 # must be power of 2, but <= 2**32
    buckets = [[] for i in xrange(n_doc_buckets)]
    bucket_sizes = array('I', [4 for i in xrange(n_doc_buckets)]) # 4 -- for 'num_docs'
    for doc_id, url in zip(xrange(1, len(docs) + 1), docs):
        bucket_n = doc_id & (n_doc_buckets - 1)
        bucket = buckets[bucket_n]
        url_size = len(url)
        bucket.append((doc_id, url_size, url))
        bucket_sizes[bucket_n] += 4 + 4 + url_size
    with open('doc_ids', 'w') as f:
        array('I', [n_doc_buckets]).write(f)
        array('H', bucket_sizes).write(f)
        for bucket in buckets:
            num_urls = len(bucket)
            array('I', [num_urls]).write(f)
            if num_urls:
                for (doc_id, url_size, url) in bucket:
                    array('I', [doc_id, url_size]).write(f)
                    array('B', url).write(f)
    # t2 = datetime.now()
    # time_elapsed_ser = (t2 - t1).total_seconds()
    # print("serializing done in {} s".format(time_elapsed_ser))
    # print("total: {} s".format(time_elapsed  + time_elapsed_ser))
